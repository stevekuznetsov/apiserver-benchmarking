package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"apiserver-benchmarking/pkg/pprof"
	"github.com/sirupsen/logrus"
)

type options struct {
	baseUrl  string
	name     string
	metric   string
	output   string
	query    string
	header   string
	interval time.Duration
}

func defaultOptions() *options {
	return &options{
		interval: 5 * time.Second,
	}
}

func bindOptions(fs *flag.FlagSet, defaults *options) *options {
	fs.StringVar(&defaults.baseUrl, "pprof-url", defaults.baseUrl, "URL at which pprof debug endpoints are exposed.")
	fs.StringVar(&defaults.name, "name", defaults.name, "Name for the process we're profiling.")
	fs.StringVar(&defaults.metric, "metric", defaults.metric, "Name for the metric we're profiling.")
	fs.StringVar(&defaults.query, "query", defaults.query, "Query to pass to pprof.")
	fs.StringVar(&defaults.header, "header", defaults.header, "Header to pass to pprof.")
	fs.StringVar(&defaults.output, "output", defaults.output, "Directory to write output data to.")
	fs.DurationVar(&defaults.interval, "interval", defaults.interval, "Sampling interval.")
	return defaults
}

func (o *options) validate() error {
	if o.baseUrl == "" {
		return errors.New("--pprof-url is required")
	}
	if o.name == "" {
		return errors.New("--name is required")
	}
	if o.metric == "" {
		return errors.New("--metric is required")
	}
	if o.output == "" {
		return errors.New("--output is required")
	}
	if o.query != "" && len(strings.Split(o.query, "=")) != 2 {
		return fmt.Errorf("--query must be in key=value form, if passed")
	}
	if o.header != "" && len(strings.Split(o.header, "=")) != 2 {
		return fmt.Errorf("--header must be in key=value form, if passed")
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

	query, header := map[string]string{}, map[string]string{}
	if opts.query != "" {
		parts := strings.Split(opts.query, "=")
		if len(parts) != 2 {
			logrus.Fatalf("invalid --query: %q, must be in k=v form", opts.query)
		}
		k, v := parts[0], parts[1]
		query[k] = v
	}
	if opts.header != "" {
		parts := strings.Split(opts.header, "=")
		if len(parts) != 2 {
			logrus.Fatalf("invalid --header: %q, must be in k=v form", opts.header)
		}
		k, v := parts[0], parts[1]
		header[k] = v
	}

	if err := pprof.Fetch(ctx, logrus.WithFields(logrus.Fields{}), opts.baseUrl, opts.output, opts.name, opts.metric, opts.interval, query, header); err != nil {
		logrus.WithError(err).Fatal("failed to fetch pprof metrics")
	}
}
