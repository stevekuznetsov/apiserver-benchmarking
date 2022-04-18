package main

import (
	"errors"
	"flag"
	"os"

	"apiserver-benchmarking/pkg/pprof"
	"github.com/sirupsen/logrus"
)

type options struct {
	inputDir string
	output   string
}

func defaultOptions() *options {
	return &options{}
}

func bindOptions(fs *flag.FlagSet, defaults *options) *options {
	fs.StringVar(&defaults.inputDir, "input", defaults.inputDir, "Directory with pprof profiles to read.")
	fs.StringVar(&defaults.output, "output", defaults.output, "File to write output data to.")
	return defaults
}

func (o *options) validate() error {
	if o.inputDir == "" {
		return errors.New("--input is required")
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
	if err := pprof.Digest(opts.inputDir, opts.output); err != nil {
		logrus.WithError(err).Fatal("failed to digest pprof data")
	}
}
