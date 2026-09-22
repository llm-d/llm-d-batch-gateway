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
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"

	"github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/logging"
)

const (
	defaultStatusTTLSec = 24 * 60 * 60
	statusSweepInterval = 30 * time.Second
)

const setStatusSQL = `INSERT INTO batch_status (job_id, data, expires_at)
VALUES ($1, $2, $3)
ON CONFLICT (job_id) DO UPDATE
SET data = EXCLUDED.data, expires_at = EXCLUDED.expires_at`

const getStatusSQL = `SELECT data FROM batch_status
WHERE job_id = $1 AND expires_at > EXTRACT(EPOCH FROM NOW())::BIGINT`

const deleteStatusSQL = `DELETE FROM batch_status WHERE job_id = $1`

const purgeExpiredStatusSQL = `DELETE FROM batch_status
WHERE expires_at <= EXTRACT(EPOCH FROM NOW())::BIGINT`

// PostgresBatchStatusClient implements api.BatchStatusClient while preserving
// its opaque payload and TTL contract. Temporary progress data is stored in a
// dedicated table so it cannot overwrite the authoritative batch status JSON.
type PostgresBatchStatusClient struct {
	pool      pgxPool
	logger    logr.Logger
	closeOnce sync.Once

	sweepCancel context.CancelFunc
	sweepDone   chan struct{}
}

var _ api.BatchStatusClient = (*PostgresBatchStatusClient)(nil)

func NewPostgresBatchStatusClient(ctx context.Context, config *PostgreSQLConfig, logger logr.Logger) (*PostgresBatchStatusClient, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if config == nil {
		return nil, fmt.Errorf("postgresql config cannot be nil")
	}

	pool, err := newPool(ctx, config)
	if err != nil {
		return nil, err
	}
	if _, err := pool.Exec(ctx, batchSchemaSql); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to apply batch status schema: %w", err)
	}

	c := &PostgresBatchStatusClient{pool: pool, logger: logger}
	c.startStatusSweeper()
	logger.V(logging.INFO).Info("NewPostgresBatchStatusClient: client created successfully")
	return c, nil
}

func (c *PostgresBatchStatusClient) Close() error {
	c.closeOnce.Do(func() {
		if c.sweepCancel != nil {
			c.sweepCancel()
			<-c.sweepDone
		}
		if c.pool != nil {
			c.pool.Close()
		}
	})
	return nil
}

func (c *PostgresBatchStatusClient) StatusSet(ctx context.Context, id string, ttl int, data []byte) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if id == "" {
		return fmt.Errorf("empty ID")
	}
	if len(data) == 0 {
		return fmt.Errorf("empty data")
	}
	if ttl <= 0 {
		ttl = defaultStatusTTLSec
	}
	_, err := c.pool.Exec(ctx, setStatusSQL, id, data, time.Now().Unix()+int64(ttl))
	if err != nil {
		return fmt.Errorf("StatusSet: %w", err)
	}
	return nil
}

func (c *PostgresBatchStatusClient) StatusGet(ctx context.Context, id string) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if id == "" {
		return nil, fmt.Errorf("empty ID")
	}
	rows, err := c.pool.Query(ctx, getStatusSQL, id)
	if err != nil {
		return nil, fmt.Errorf("StatusGet: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("StatusGet: %w", err)
		}
		return nil, nil
	}
	var data []byte
	if err := rows.Scan(&data); err != nil {
		return nil, fmt.Errorf("StatusGet: scan: %w", err)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("StatusGet: %w", err)
	}
	return data, nil
}

func (c *PostgresBatchStatusClient) StatusDelete(ctx context.Context, id string) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if id == "" {
		return 0, fmt.Errorf("empty ID")
	}
	result, err := c.pool.Exec(ctx, deleteStatusSQL, id)
	if err != nil {
		return 0, fmt.Errorf("StatusDelete: %w", err)
	}
	return int(result.RowsAffected()), nil
}

func (c *PostgresBatchStatusClient) startStatusSweeper() {
	ctx, cancel := context.WithCancel(context.Background())
	c.sweepCancel = cancel
	c.sweepDone = make(chan struct{})
	go func() {
		defer close(c.sweepDone)
		ticker := time.NewTicker(statusSweepInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if _, err := c.purgeExpiredStatuses(ctx); err != nil {
					c.logger.V(logging.INFO).Info("status sweeper: purge failed", "err", err.Error())
				}
			}
		}
	}()
}

func (c *PostgresBatchStatusClient) purgeExpiredStatuses(ctx context.Context) (int64, error) {
	result, err := c.pool.Exec(ctx, purgeExpiredStatusSQL)
	if err != nil {
		return 0, fmt.Errorf("purge expired statuses: %w", err)
	}
	return result.RowsAffected(), nil
}
