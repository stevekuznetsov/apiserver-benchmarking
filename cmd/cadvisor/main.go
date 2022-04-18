package main

import (
	"context"
	"errors"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"

	"apiserver-benchmarking/pkg/cadvisor"
)

type options struct {
	baseUrl   string
	container string
	output    string
}

func defaultOptions() *options {
	return &options{}
}

func bindOptions(fs *flag.FlagSet, defaults *options) *options {
	fs.StringVar(&defaults.baseUrl, "cadvisor-url", defaults.baseUrl, "URL at which cAdvisor is running.")
	fs.StringVar(&defaults.container, "container", defaults.container, "Name for the container we're profiling.")
	fs.StringVar(&defaults.output, "output", defaults.output, "Directory to write output data to.")
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

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer func() {
		cancel()
	}()

	if err := cadvisor.Fetch(ctx, logrus.WithFields(logrus.Fields{}), opts.baseUrl, opts.output, opts.container, 5*time.Second); err != nil {
		logrus.WithError(err).Fatal("failed to fetch cAdvisor metrics")
	}
}
