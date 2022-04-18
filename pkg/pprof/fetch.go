package pprof

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"
)

func Fetch(ctx context.Context, logger *logrus.Entry, baseUrl, outputDir, container, metric string, interval time.Duration, query, header map[string]string) error {
	logger = logger.WithFields(logrus.Fields{"container": container, "datasource": "pprof", "metric": metric})

	u, err := url.Parse(baseUrl)
	if err != nil {
		return fmt.Errorf("invalid url: %w", err)
	}

	baseDir := filepath.Join(outputDir, "pprof", metric, container)
	if err := os.MkdirAll(baseDir, 0777); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr}

	tick := time.Tick(interval)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick:
			u.Path = "/debug/pprof/" + metric
			q := u.Query()
			for k, v := range query {
				q.Set(k, v)
			}
			u.RawQuery = q.Encode()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
			if err != nil {
				logger.WithError(err).Error("error creating request")
				continue
			}
			for k, v := range header {
				req.Header.Set(k, v)
			}
			resp, err := client.Do(req)
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					logger.WithError(err).Error("error fetching")
				}
				continue
			}
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				logger.WithError(err).Error("error reading body")
				continue
			}
			if err := resp.Body.Close(); err != nil {
				logger.WithError(err).Error("error closing body")
				continue
			}
			if resp.StatusCode != http.StatusOK {
				logger.WithError(err).Errorf("got %d, not 200: %v", resp.StatusCode, string(body))
				continue
			}
			f, err := ioutil.TempFile(baseDir, "*")
			if err != nil {
				logger.WithError(err).Error("error creating new file")
				continue
			}
			if n, err := f.Write(body); err != nil {
				logger.WithError(err).Error("error writing file")
			} else if n != len(body) {
				logger.Errorf("wrote %d bytes to file, expected %d", n, len(body))
			}
			if err := f.Close(); err != nil {
				logger.WithError(err).Error("error closing file")
			}
		}
	}
}
