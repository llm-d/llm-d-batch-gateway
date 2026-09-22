//go:build simulation

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

package simulation

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	asyncapi "github.com/llm-d/llm-d-async/api"
	goredis "github.com/redis/go-redis/v9"
)

const (
	asyncPool          = "sim-pool"
	asyncRequestQueue  = "llm-d-async:requests:" + asyncPool
	asyncResultQueue   = "llm-d-async:results:" + asyncPool
	simRedisAddr       = "127.0.0.1:16379"
	simVCRBridgeURL    = "http://127.0.0.1:18001"
	bridgePollInterval = 200 * time.Millisecond
)

// asyncBridge plays the llm-d-async worker fleet for the compose stack: it
// drains RequestMessages from the pool's request queue, forwards each to the
// vcr model server, and pushes the ResultMessage onto the result queue. The
// processor under test only ever sees the real queue protocol.
type asyncBridge struct {
	t      *testing.T
	rdb    *goredis.Client
	served atomic.Int64
	wg     sync.WaitGroup
}

func startAsyncBridge(ctx context.Context, t *testing.T) *asyncBridge {
	t.Helper()
	b := &asyncBridge{
		t:   t,
		rdb: goredis.NewClient(&goredis.Options{Addr: simRedisAddr}),
	}
	t.Cleanup(func() {
		b.wg.Wait()
		_ = b.rdb.Close()
	})
	b.wg.Add(1)
	go b.run(ctx)
	return b
}

func (b *asyncBridge) run(ctx context.Context) {
	defer b.wg.Done()
	ticker := time.NewTicker(bridgePollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			members, err := b.rdb.ZPopMin(ctx, asyncRequestQueue, 16).Result()
			if err != nil || len(members) == 0 {
				continue
			}
			for _, m := range members {
				raw, ok := m.Member.(string)
				if !ok {
					continue
				}
				b.wg.Add(1)
				go func() {
					defer b.wg.Done()
					b.serve(ctx, raw)
				}()
			}
		}
	}
}

// serve forwards one queued request to the model server and enqueues the
// result. Failures become error results, matching worker behavior.
func (b *asyncBridge) serve(ctx context.Context, raw string) {
	var ir asyncapi.InternalRequest
	if err := json.Unmarshal([]byte(raw), &ir); err != nil {
		b.t.Logf("async bridge: undecodable request dropped: %v", err)
		return
	}
	msg, ok := ir.PublicRequest.(*asyncapi.RequestMessage)
	if !ok {
		b.t.Logf("async bridge: unexpected request kind %T", ir.PublicRequest)
		return
	}
	resultQueue := ir.ResultQueueName
	if resultQueue == "" {
		resultQueue = asyncResultQueue
	}

	result := asyncapi.ResultMessage{ID: msg.ID}
	body, err := json.Marshal(msg.Payload)
	if err == nil {
		var resp *http.Response
		req, reqErr := http.NewRequestWithContext(ctx, http.MethodPost,
			simVCRBridgeURL+msg.Endpoint, bytes.NewReader(body))
		if reqErr == nil {
			req.Header.Set("Content-Type", "application/json")
			resp, err = http.DefaultClient.Do(req)
		} else {
			err = reqErr
		}
		if err == nil {
			payload, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr != nil {
				err = readErr
			} else {
				result.StatusCode = resp.StatusCode
				result.Payload = string(payload)
			}
		}
	}
	if err != nil {
		result.ErrorCode = "bridge_error"
		result.ErrorMessage = err.Error()
	}

	data, err := json.Marshal(result)
	if err != nil {
		b.t.Logf("async bridge: marshal result: %v", err)
		return
	}
	if err := b.rdb.LPush(context.WithoutCancel(ctx), resultQueue, string(data)).Err(); err != nil {
		b.t.Logf("async bridge: push result: %v", err)
		return
	}
	b.served.Add(1)
}

// resultQueueLen reports how many results are waiting unconsumed.
func (b *asyncBridge) resultQueueLen(ctx context.Context) int64 {
	n, err := b.rdb.LLen(ctx, asyncResultQueue).Result()
	if err != nil {
		return -1
	}
	return n
}

func (b *asyncBridge) String() string {
	return fmt.Sprintf("asyncBridge(served=%d)", b.served.Load())
}
