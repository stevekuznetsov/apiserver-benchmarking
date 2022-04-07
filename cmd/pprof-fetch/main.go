package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
)

type options struct {
	baseUrl  string
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
	u, err := url.Parse(opts.baseUrl)
	if err != nil {
		logrus.WithError(err).Fatal("invalid url")
	}
	baseDir := filepath.Join(opts.output, opts.metric)
	if err := os.MkdirAll(baseDir, 0777); err != nil {
		logrus.WithError(err).Fatal("failed to create output directory")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer func() {
		cancel()
	}()

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr}

	tick := time.Tick(opts.interval)
	logrus.Infof("starting to poll at %s", opts.interval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick:
			u.Path = "/debug/pprof/" + opts.metric
			if opts.query != "" {
				parts := strings.Split(opts.query, "=")
				k, v := parts[0], parts[1]
				q := u.Query()
				q.Set(k, v)
				u.RawQuery = q.Encode()
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
			if err != nil {
				logrus.WithError(err).Error("error creating request")
				continue
			}
			if opts.header != "" {
				parts := strings.Split(opts.header, "=")
				k, v := parts[0], parts[1]
				req.Header.Set(k, v)
			}
			resp, err := client.Do(req)
			if err != nil {
				logrus.WithError(err).Error("error fetching")
				continue
			}
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				logrus.WithError(err).Error("error reading body")
				continue
			}
			if err := resp.Body.Close(); err != nil {
				logrus.WithError(err).Error("error closing body")
				continue
			}
			if resp.StatusCode != http.StatusOK {
				logrus.WithError(err).Errorf("got %d, not 200: %v", resp.StatusCode, string(body))
				continue
			}
			f, err := ioutil.TempFile(baseDir, "*")
			if err != nil {
				logrus.WithError(err).Error("error creating new file")
				continue
			}
			if n, err := f.Write(body); err != nil {
				logrus.WithError(err).Error("error writing file")
			} else if n != len(body) {
				logrus.Errorf("wrote %d bytes to file, expected %d", n, len(body))
			}
			if err := f.Close(); err != nil {
				logrus.WithError(err).Error("error closing file")
			}
		}
	}
}
