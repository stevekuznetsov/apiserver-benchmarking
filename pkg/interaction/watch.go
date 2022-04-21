package interaction

import (
	"context"
	"errors"
	"sync"
	"time"

	"apiserver-benchmarking/pkg/config"
	"github.com/sirupsen/logrus"
)

func Watch(ctx context.Context, im *interactionManager, cfg *config.Interact) error {
	im.d = &configmapDelegate{
		fillSize: cfg.Watch.FillSize,
		client:   im.client,
	}
	ctx, cancel := context.WithTimeout(ctx, cfg.Watch.Duration.Duration)
	defer cancel()
	wg := &sync.WaitGroup{}
	// first, begin watching
	watchObjects(ctx, im, cfg.Watch, wg)

	// next, add the object we're interested in
	if err := seedForWatching(ctx, im, cfg.Watch); err != nil {
		return err
	}
	// finally, start the threads that will generate events
	updateForWatching(ctx, im, cfg.Watch, wg)

	wg.Wait()
	return nil
}

func seedForWatching(ctx context.Context, im *interactionManager, cfg *config.Watch) error {
	wg := &sync.WaitGroup{}
	operations := make(chan struct{})
	for i := 0; i < cfg.Parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case _, ok := <-operations:
					if !ok {
						return
					}
					if err := im.create(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
						logrus.WithError(err).Error("failed to interact with the API server")
					}
				}
			}
		}()
	}

	start := time.Now()
	for i := 0; i < cfg.Count; i++ {
		start = display(i, cfg.Count, start)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case operations <- struct{}{}:
		}
	}
	close(operations)
	wg.Wait()
	return nil
}

func updateForWatching(ctx context.Context, im *interactionManager, cfg *config.Watch, wg *sync.WaitGroup) {
	for i := 0; i < cfg.Parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				if err := im.update(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
					logrus.WithError(err).Error("failed to interact with the API server")
				}
			}
		}()
	}
}

func watchObjects(ctx context.Context, im *interactionManager, cfg *config.Watch, wg *sync.WaitGroup) {
	for _, namespace := range im.namespaces {
		for i := 0; i < cfg.WatchersPerPartition; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := im.watchNamespaced(ctx, namespace); err != nil && !errors.Is(err, context.DeadlineExceeded) {
					logrus.WithError(err).Error("failed to interact with the API server")
				}
			}()
		}
	}
}
