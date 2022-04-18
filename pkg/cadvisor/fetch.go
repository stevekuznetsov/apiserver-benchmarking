package cadvisor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"

	client "github.com/google/cadvisor/client/v2"
	api "github.com/google/cadvisor/info/v2"
	"github.com/sirupsen/logrus"
)

func Fetch(ctx context.Context, logger *logrus.Entry, baseUrl, outputDir, container string, interval time.Duration) error {
	logger = logger.WithFields(logrus.Fields{"container": container, "datasource": "cadvisor"})
	c, err := client.NewClient(baseUrl)
	if err != nil {
		return fmt.Errorf("failed to create cAdvisor client: %w", err)
	}

	var data []*api.ContainerStats
	dataChan := make(chan *api.ContainerStats, 100)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case datum, ok := <-dataChan:
				if !ok {
					return
				}
				data = append(data, datum)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := fetch(ctx, logger, interval, c, container, dataChan); err != nil && !errors.Is(err, context.Canceled) {
			logger.WithError(err).Error("failed to fetch cAdvisor data")
		}
	}()

	wg.Wait()
	close(dataChan)

	logger.Info("finished collecting data")
	raw, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to serialize cAdvisor output: %w", err)
	}
	outputDir = filepath.Join(outputDir, "cadvisor")
	if err := os.MkdirAll(outputDir,  0777); err != nil {
		return fmt.Errorf("failed to create directory for cAdvisor output: %w", err)
	}
	outputPath := filepath.Join(outputDir, container + ".json")
	if err := ioutil.WriteFile(outputPath, raw, 0666); err != nil {
		return fmt.Errorf("failed to write cAdvisor output: %w", err)
	}
	logger.Infof("wrote output to %s, exiting", outputPath)
	return nil
}

func fetch(ctx context.Context, logger *logrus.Entry, interval time.Duration, c *client.Client, container string, output chan<- *api.ContainerStats) error {
	previous := time.Time{}
	tick := time.Tick(interval)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick:
			infos, err := c.Stats(container, &api.RequestOptions{
				IdType:    "podman",
				Count:     -1,
				Recursive: false,
				MaxAge:    &interval,
			})
			if err != nil {
				logger.WithError(err).Warn("failed to request data")
			}
			if len(infos) > 1 {
				logger.Warnf("expected status for one container, for %d", len(infos))
			}
			for _, containerInfo := range infos {
				for _, stat := range containerInfo.Stats {
					if stat.Timestamp.After(previous) {
						// we only want to keep some of this data
						specifics := &api.ContainerStats{
							Timestamp: stat.Timestamp.UTC(), // numpy is deprecating parsing times with zones?
							Cpu:       stat.Cpu,
							Memory:    stat.Memory,
						}
						previous = specifics.Timestamp
						output <- specifics
					}
				}
			}
		}
	}
}
