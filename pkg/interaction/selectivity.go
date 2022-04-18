package interaction

import (
	"context"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"apiserver-benchmarking/pkg/config"
)

func Select(ctx context.Context, im *interactionManager, cfg *config.Interact) error {
	// first, add the objects we're interested in
	batches, err := seedForSelectivity(ctx, im, cfg.Selectivity)
	if err != nil {
		return err
	}
	// then, run the selectivity tests
	return selectObjects(ctx, im, cfg, batches)
}

func seedForSelectivity(ctx context.Context, im *interactionManager, cfg *config.Selectivity) ([]int, error) {
	wg := &sync.WaitGroup{}
	operations := make(chan int)
	for i := 0; i < cfg.Parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case nodeName, ok := <-operations:
					if !ok {
						return
					}
					if err := im.createWithNodeName(ctx, strconv.Itoa(nodeName)); err != nil {
						logrus.WithError(err).Error("failed to interact with the API server")
					}
				}
			}
		}()
	}

	// we want to create sets of Pods with the same node name for us to select in the future,
	// but we should do so at random; let's pre-allocate our order and just iterate through it
	names := make([]int, cfg.Count)
	var batches []int
	batchSize := cfg.Minimum
	factor := 10
	var index int
	for {
		if index+batchSize < len(names) {
			batches = append(batches, batchSize)
			// we have enough space to add this batch
			for i := 0; i < batchSize; i++ {
				names[index+i] = batchSize
			}
			index = index + batchSize
			batchSize = batchSize * factor
		} else {
			if factor == 10 {
				// try a smaller step
				batchSize = batchSize / factor
				factor = 2
				batchSize = batchSize * factor
				continue
			}
			// let's just pad the rest with random data
			for i := 0; i < len(names)-index; i++ {
				names[index+i] = im.r.Intn(math.MaxInt-batchSize-1) + batchSize + 1 // generate non-overlapping numbers
			}
			break
		}
	}
	im.r.Shuffle(len(names), func(i, j int) {
		names[i], names[j] = names[j], names[i]
	})

	start := time.Now()
	for i, name := range names {
		start = display(i, cfg.Count, start)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case operations <- name:
		}
	}
	close(operations)
	wg.Wait()
	return batches, nil
}

func selectObjects(ctx context.Context, im *interactionManager, cfg *config.Interact, batches []int) error {
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
					if err := im.listWithNodeName(ctx, strconv.Itoa(batches[im.r.Intn(len(batches))])); err != nil {
						logrus.WithError(err).Error("failed to interact with the API server")
					}
				}
			}
		}()
	}

	start := time.Now()
	for i := 0; i < cfg.Operations; i++ {
		start = display(i, cfg.Operations, start)
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
