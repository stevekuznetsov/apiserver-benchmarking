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

	"apiserver-benchmarking/pkg/containers"
	"apiserver-benchmarking/pkg/interaction"
	"apiserver-benchmarking/pkg/pprof"
	"github.com/sirupsen/logrus"

	"apiserver-benchmarking/pkg/config"
)

type options struct {
	configFile string
}

func defaultOptions() *options {
	return &options{}
}

func bindOptions(fs *flag.FlagSet, defaults *options) *options {
	fs.StringVar(&defaults.configFile, "config", defaults.configFile, "Path to configuration file.")
	return defaults
}

func (o *options) validate() error {
	if o.configFile == "" {
		return errors.New("--config is required")
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

	raw, err := ioutil.ReadFile(opts.configFile)
	if err != nil {
		logrus.WithError(err).Fatal("could not read config file")
	}
	var cfg config.Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		logrus.WithError(err).Fatal("could not deserialize config")
	}

	if cfg.Setup == nil {
		logrus.Fatal("no setup configured")
	}

	if err := os.RemoveAll(cfg.Setup.OutputDir); err != nil {
		logrus.WithError(err).Fatal("could not clear output dir")
	}
	if err := os.MkdirAll(cfg.Setup.OutputDir, 0777); err != nil {
		logrus.WithError(err).Fatal("could not create output dir")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer func() {
		cancel()
	}()

	cm, err := containers.NewManager(ctx, logrus.WithField("component", "containers"))
	if err != nil {
		logrus.WithError(err).Fatal("could not initialize container manager")
	}
	logrus.DeferExitHandler(func() {
		if err := cm.Cleanup(cfg.Setup.OutputDir); err != nil {
			logrus.WithError(err).Error("failed to clean up containers")
		}

		if err := pprof.DigestAll(cfg.Setup.OutputDir); err != nil {
			logrus.WithError(err).Error("failed to digest pprof data")
		}
	})

	if err := cm.Deploy(cfg.Setup); err != nil {
		logrus.WithError(err).Fatal("could not deploy containers")
	}

	client, err := cm.APIServerClient()
	if err != nil {
		logrus.WithError(err).Fatal("could not get API server client")
	}

	fillSize := 1
	if cfg.Seed != nil {
		fillSize = cfg.Seed.FillSize // TODO: poor factoring here ... can we move all size info to seed step?
	}
	partitions := 1
	if cfg.Interact != nil && cfg.Interact.Watch != nil {
		partitions = cfg.Interact.Watch.Partitions
	}
	im, err := interaction.NewManager(ctx, fillSize, partitions, client)
	if err != nil {
		logrus.WithError(err).Fatal("could not create interaction manager")
	}
	logrus.DeferExitHandler(func() {
		if err := im.WriteOutput(cfg.Setup.OutputDir); err != nil {
			logrus.WithError(err).Error("failed to write interaction output")
		}
	})

	// we want to make sure that we've started fetching data for metrics before we start interacting
	select {
	case <-time.After(5 * time.Second):
	case <-ctx.Done():
	}

	if cfg.Seed != nil {
		logrus.Info("seeding the API server...")
		err := interaction.Seed(ctx, im, cfg.Seed)
		logrus.Info("done seeding the API server...")
		if err != nil {
			logrus.WithError(err).Fatal("failed to seed the API server")
		}
	}

	if cfg.Interact != nil {
		logrus.Info("interacting with the API server...")
		var err error
		if cfg.Interact.Throughput != nil {
			err = interaction.Interact(ctx, im, cfg.Interact)
		}
		if cfg.Interact.Selectivity != nil {
			err = interaction.Select(ctx, im, cfg.Interact)
		}
		if cfg.Interact.Watch != nil {
			err = interaction.Watch(ctx, im, cfg.Interact)
		}
		logrus.Info("done interacting with the API server...")
		if err != nil {
			logrus.WithError(err).Fatal("failed to interact with the API server")
		}
	}

	logrus.Exit(0) // we need to explicitly call Exit() so that our handlers are called
}
