package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"
	"time"

	client "github.com/google/cadvisor/client/v2"
	api "github.com/google/cadvisor/info/v2"
	"github.com/sirupsen/logrus"
)

type options struct {
	baseUrl   string
	container string
	output string
}

func defaultOptions() *options {
	return &options{}
}

func bindOptions(fs *flag.FlagSet, defaults *options) *options {
	fs.StringVar(&defaults.baseUrl, "cadvisor-url", defaults.baseUrl, "URL at which cAdvisor is running.")
	fs.StringVar(&defaults.container, "container", defaults.container, "Name for the container we're profiling.")
	fs.StringVar(&defaults.output, "output", defaults.output, "File to write output data to.")
	return defaults
}

func (o *options) validate() error {
	if o.baseUrl == "" {
		return errors.New("--cadvisor-url is required")
	}
	if o.container == "" {
		return errors.New("--container is required")
	}
	if o.output == "" {
		return errors.New("--output is required")
	}
	return nil
}

func main() {
	opts := defaultOptions()
	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	opts = bindOptions(fs, opts)
	if err := fs.Parse(os.Args[1:]); err != nil {
		logrus.WithError(err).Fatal("failed to parse arguments")
	}
	if err := opts.validate(); err != nil {
		logrus.WithError(err).Fatal("invalid options")
	}
	c, err := client.NewClient(opts.baseUrl)
	if err != nil {
		logrus.WithError(err).Fatal("failed to create cAdvisor client")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer func() {
		cancel()
	}()

	var data []*api.ContainerStats
	defer func() {
		logrus.Info("finished collecting data")
		raw, err := json.Marshal(data)
		if err != nil {
			logrus.WithError(err).Errorf("failed to serialize output")
			return
		}
		if err := ioutil.WriteFile(opts.output, raw, 0666); err != nil {
			logrus.WithError(err).Errorf("failed to write output")
		}
		logrus.Infof("wrote output to %s, exiting", opts.output)
	}()

	interval := 5 * time.Second
	tick := time.Tick(interval)
	logrus.Infof("starting to poll at %s", interval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick:
			infos, err := c.Stats(opts.container, &api.RequestOptions{
				IdType:    "podman",
				Count: -1,
				Recursive: false,
				MaxAge:    &interval,
			})
			if err != nil {
				logrus.WithError(err).Warn("failed to request data")
			}
			if len(infos) > 1 {
				logrus.Warnf("expected status for one container, for %d", len(infos))
			}
			var newData []*api.ContainerStats
			previous := time.Time{}
			if len(data) > 0 {
				previous = data[len(data)-1].Timestamp
			}
			for _, containerInfo := range infos {
				logrus.Infof("fetched %d samples", len(containerInfo.Stats))
				for _, stat := range containerInfo.Stats {
					if stat.Timestamp.After(previous) {
						// we only want to keep some of this data
						specifics := &api.ContainerStats{
							Timestamp: stat.Timestamp.UTC(), // numpy is deprecating parsing times with zones?
							Cpu: stat.Cpu,
							Memory: stat.Memory,
						}
						newData = append(newData, specifics)
					}
				}
			}
			if len(newData) == 0 {
				continue
			}
			logrus.WithFields(logrus.Fields{
				"before": previous.String(),
				"after": newData[len(newData)-1].Timestamp.String(),
			}).Infof("recorded %d new samples", len(newData))
			data = append(data, newData...)
		}
	}
}
