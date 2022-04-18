package pprof

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/pprof/profile"

	"github.com/sirupsen/logrus"
)

func Digest(inputDir, output string) error {
	// unix nanos to data type to value
	data := map[time.Time]map[string]int64{}

	logrus.Infof("Operating on profiles at %s", inputDir)
	if err := filepath.Walk(inputDir, func(path string, info os.FileInfo, err error) error {
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
		return fmt.Errorf("failed to operate on profiles: %w", err)
	}

	logrus.Info("Finished parsing profiles")
	raw, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to serialize output: %w", err)
	}
	if err := ioutil.WriteFile(output, raw, 0666); err != nil {
		return fmt.Errorf("failed to write output: %w", err)
	}
	logrus.Infof("Wrote output to %s", output)
	return nil
}

func DigestAll(baseDir string) error {
	logrus.Info("Digesting pprof profiles.")
	return filepath.Walk(filepath.Join(baseDir, "pprof"), func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		relPath, err := filepath.Rel(baseDir, path)
		if err != nil {
			logrus.WithError(err).Warn("Unexpected error finding a relative path.")
			return nil
		}
		if info.IsDir() && strings.Count(relPath, string(os.PathSeparator)) == 2 {
			if err := Digest(path, filepath.Join(path, "output.json")); err != nil {
				return err
			}
		}
		return nil
	})
}