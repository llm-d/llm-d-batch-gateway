/*
Copyright 2026 The llm-d Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package postgresql

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/llm-d/llm-d-batch-gateway/internal/util/logging"
)

// pgListener holds one dedicated connection from the pool in WaitForNotification
// on the given channel, fanning notifications out to subscribers.
// It is purely event-driven: it does not poll the database on a timer.
// On initial connect and on reconnection after a connection drop, it invokes
// onReconnect to trigger a proactive drain across all active subscribers.
type pgListener struct {
	pool        *pgxpool.Pool
	channel     string
	logger      logr.Logger
	onReconnect func()

	mu     sync.Mutex
	subs   map[int]chan string
	nextID int

	cancel    context.CancelFunc
	closeOnce sync.Once
	done      chan struct{}

	listenErrLogged bool
	deferred        atomic.Int64
}

func newPGListener(pool *pgxpool.Pool, channel string, logger logr.Logger, onReconnect func()) *pgListener {
	l := &pgListener{
		pool:        pool,
		channel:     channel,
		logger:      logger,
		onReconnect: onReconnect,
		subs:        make(map[int]chan string),
		done:        make(chan struct{}),
	}
	runCtx, cancel := context.WithCancel(context.Background())
	l.cancel = cancel
	go l.run(runCtx)
	return l
}

func (l *pgListener) subscribe() (<-chan string, func()) {
	ch := make(chan string, 100)

	l.mu.Lock()
	id := l.nextID
	l.nextID++
	l.subs[id] = ch
	l.mu.Unlock()

	unsubscribe := func() {
		l.mu.Lock()
		delete(l.subs, id)
		l.mu.Unlock()
	}
	return ch, unsubscribe
}

func (l *pgListener) close() error {
	l.closeOnce.Do(func() {
		if l.cancel != nil {
			l.cancel()
		}
		if l.done != nil {
			<-l.done
		}
	})
	return nil
}

func (l *pgListener) deliver(payload string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, ch := range l.subs {
		select {
		case ch <- payload:
		default:
			// The subscriber cannot keep up. The event is not lost — its row
			// stays in batch_events and the dispatcher's periodic rescan
			// delivers it — but log so the deferral is visible.
			l.deferred.Add(1)
			l.logger.V(logging.INFO).Info("pgListener: subscriber buffer full, deferring event to rescan", "channel", l.channel, "totalDeferred", l.deferred.Load())
		}
	}
}

func (l *pgListener) run(ctx context.Context) {
	defer close(l.done)

	for {
		if ctx.Err() != nil {
			return
		}

		conn, err := l.pool.Acquire(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			l.logger.V(logging.INFO).Info("pgListener: acquire connection failed, retrying", "err", err.Error())
			select {
			case <-ctx.Done():
				return
			case <-time.After(500 * time.Millisecond):
				continue
			}
		}

		l.listenLoop(ctx, conn)
		conn.Release()

		if ctx.Err() != nil {
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (l *pgListener) listenLoop(ctx context.Context, conn *pgxpool.Conn) {
	if _, err := conn.Exec(ctx, "LISTEN "+l.channel); err != nil {
		if !l.listenErrLogged {
			l.listenErrLogged = true
			l.logger.Error(err, "pgListener: LISTEN failed", "channel", l.channel)
		}
		return
	}

	l.listenErrLogged = false
	l.logger.V(logging.INFO).Info("pgListener: listening", "channel", l.channel)

	// Trigger proactive drain on successful connect/reconnect
	if l.onReconnect != nil {
		l.onReconnect()
	}

	for {
		if ctx.Err() != nil {
			return
		}

		n, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			l.logger.V(logging.INFO).Info("pgListener: WaitForNotification ended, reconnecting", "err", err.Error())
			return
		}

		l.deliver(n.Payload)
	}
}
