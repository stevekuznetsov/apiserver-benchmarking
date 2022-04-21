package interaction

import (
	"context"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	kerrors "k8s.io/apimachinery/pkg/api/errors"

	"apiserver-benchmarking/pkg/config"
)

func Interact(ctx context.Context, im *interactionManager, cfg *config.Interact) error {
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
					if err := do(ctx, im, cfg); err != nil {
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


type action uint8

const (
	actionCreate action = iota
	actionGet
	actionList
	actionUpdate
	actionDelete
)

func do(ctx context.Context, im *interactionManager, cfg *config.Interact) error {
	prop := cfg.Throughput
	var actionOptions []action
	for actionType, count := range map[action]int{
		actionCreate: prop.Create,
		actionGet:    prop.Get,
		actionList:   prop.List,
		actionUpdate: prop.Update,
		actionDelete: prop.Delete,
	} {
		for j := 0; j < count; j++ {
			actionOptions = append(actionOptions, actionType)
		}
	}
	actions := map[action]func(context.Context) error{
		actionCreate: im.create,
		actionGet:    im.get,
		actionList:   im.list,
		actionUpdate: im.update,
		actionDelete: im.delete,
	}
	if err := actions[actionOptions[im.r.Intn(len(actionOptions))]](ctx); err != nil && !kerrors.IsNotFound(err) {
		return err
	}
	return nil
}