package main

import (
	"encoding/json"
	"errors"
	"flag"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/google/pprof/profile"
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

	// unix nanos to data type to value
	data := map[time.Time]map[string]int64{}

	logrus.Infof("operating on profiles at %s", opts.inputDir)
	if err := filepath.Walk(opts.inputDir, func(path string, info os.FileInfo, err error) error {
		if info == nil || info.IsDir() {
			return nil // we have a flat hierarchy
		}
		if err != nil {
			return err
		}

		f, err := os.Open(path)
		if err != nil {
			return err
		}
		p, err := profile.Parse(f)
		if err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}

		dataByType := map[string]int64{}
		unitByType := map[string]string{}
		for _, sampleType := range p.SampleType {
			dataByType[sampleType.Type] = 0
			if previous, recorded := unitByType[sampleType.Type]; !recorded {
				unitByType[sampleType.Type] = sampleType.Unit
			} else if previous != sampleType.Unit {
				logrus.WithFields(logrus.Fields{"previous": previous, "current": sampleType.Unit}).Warnf("unit disagreement for sample type %s", sampleType.Type)
			}
		}

		for _, sample := range p.Sample {
			for idx, value := range sample.Value {
				dataByType[p.SampleType[idx].Type] += value
			}
		}

		data[time.Unix(0, p.TimeNanos).UTC()] = dataByType
		return nil
	}); err != nil {
		logrus.WithError(err).Fatal("failed to operate on profiles")
	}

	logrus.Info("finished parsing profiles")
	raw, err := json.Marshal(data)
	if err != nil {
		logrus.WithError(err).Errorf("failed to serialize output")
		return
	}
	if err := ioutil.WriteFile(opts.output, raw, 0666); err != nil {
		logrus.WithError(err).Errorf("failed to write output")
	}
	logrus.Infof("wrote output to %s, exiting", opts.output)
}
