package interaction

import (
	"context"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"apiserver-benchmarking/pkg/config"
)

func Seed(ctx context.Context, im *interactionManager, cfg *config.Seed) error {
	im.d = &configmapDelegate{
		fillSize: im.fillSize,
		client:   im.client,
	}
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
					if err := im.create(ctx); err != nil {
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

func display(i, max int, start time.Time) time.Time {
	if (i+1)%(max/10) == 0 {
		logrus.Infof("progress: %d/%d (%.0f%%); %s per ", i+1, max, 100*(float64(i+1)/float64(max)), time.Since(start)/time.Duration(max/10))
		return time.Now()
	}
	return start
}