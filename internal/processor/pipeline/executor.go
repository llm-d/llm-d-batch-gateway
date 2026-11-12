package pipeline

import (
	"context"
	"errors"
	"sync"

	"github.com/go-logr/logr"
	"golang.org/x/sync/errgroup"

	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
)

// JobExecutor orchestrates the execution pipeline.
type JobExecutor struct {
	source     RequestSource
	dispatcher RequestDispatcher
	collector  *ResultCheckpointer
	tracker    *ProgressTracker
	logger     logr.Logger
}

type JobExecutorConfig struct {
	Source     RequestSource
	Dispatcher RequestDispatcher
	Collector  *ResultCheckpointer
	Tracker    *ProgressTracker
	Logger     logr.Logger
}

func NewJobExecutor(cfg JobExecutorConfig) *JobExecutor {
	return &JobExecutor{
		source:     cfg.Source,
		dispatcher: cfg.Dispatcher,
		collector:  cfg.Collector,
		tracker:    cfg.Tracker,
		logger:     cfg.Logger,
	}
}

func (je *JobExecutor) Execute(ctx context.Context) (*openai.BatchRequestCounts, error) {
	g, ctx := errgroup.WithContext(ctx)

	dispatchCtx, dispatchCancel := context.WithCancel(ctx)
	defer dispatchCancel()

	je.collector.setPersistenceFailureHook(sync.OnceFunc(dispatchCancel))

	requestCh := make(chan RequestItem)
	resultCh := make(chan ResultItem, defaultResultBuffer)

	trackerCtx, trackerCancel := context.WithCancel(ctx)
	trackerDone := make(chan struct{})
	go func() {
		err := je.tracker.Run(trackerCtx)
		if err != nil {
			je.logger.Error(err, "error on ProgressTracker shutdown for JobExecutor")
		}
		close(trackerDone)
	}()

	g.Go(func() error { return je.dispatcher.Run(dispatchCtx, requestCh, resultCh) })

	var collectorErr error
	g.Go(func() error {
		collectorErr = je.collector.Drain(ctx, resultCh)
		return collectorErr
	})

	g.Go(func() error { return je.source.Produce(dispatchCtx, requestCh) })

	err := g.Wait()

	// When onPersistenceFailure cancels dispatchCtx, the source returns
	// context.Canceled before the collector finishes draining resultCh.
	// errgroup records the first non-nil error (source's cancel), masking
	// the root cause. Prefer the collector's persistence error.
	if errors.Is(err, context.Canceled) && collectorErr != nil {
		err = collectorErr
	}

	trackerCancel()
	<-trackerDone

	return je.tracker.Counts(), err
}
