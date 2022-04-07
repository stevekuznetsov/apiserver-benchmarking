package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/utils/pointer"
)

type options struct {
	configFile   string
	apiserverUrl string
	token        string
	eventsFile   string

	apiserverFlags bool
}

func defaultOptions() *options {
	return &options{}
}

func bindOptions(fs *flag.FlagSet, defaults *options) *options {
	fs.StringVar(&defaults.apiserverUrl, "apiserver-url", defaults.apiserverUrl, "URL at which API server endpoints are exposed.")
	fs.StringVar(&defaults.token, "token", defaults.token, "Token to use for the API.")
	fs.StringVar(&defaults.configFile, "config", defaults.configFile, "Path to configuration file.")
	fs.StringVar(&defaults.eventsFile, "events", defaults.eventsFile, "Path to file where events will be written.")
	fs.BoolVar(&defaults.apiserverFlags, "print-apiserver-flags", defaults.apiserverFlags, "Print the flags that should be passed to the API server and exit.")
	return defaults
}

func (o *options) validate() error {
	if o.apiserverUrl == "" && !o.apiserverFlags {
		return errors.New("--apiserver-url is required")
	}
	if o.token == "" && !o.apiserverFlags {
		return errors.New("--token is required")
	}
	if o.configFile == "" {
		return errors.New("--config is required")
	}
	if o.eventsFile == "" && !o.apiserverFlags {
		return errors.New("--events is required")
	}
	return nil
}

type Config struct {
	// Setup configures the infrastructure on which we will be running.
	Setup *Setup `json:"setup,omitempty"`
	// Seed configures the pre-benchmark steps to seed the API server.
	Seed *Seed `json:"seed,omitempty"`
	// Interact configures the benchmark steps to interact with the API server.
	Interact *Interact `json:"interact,omitempty"`
}

type Setup struct {
	// StorageBackend is the storage backend to use, either etcd3 or crdb. Defaults to etcd3.
	StorageBackend string `json:"storage_backend,omitempty"`
	// WatchCacheEnabled determines if the watch cache should be enabled. Defaults to false.
	WatchCacheEnabled bool `json:"watch_cache_enabled,omitempty"`
}

type Seed struct {
	// FillSize is the size (in bytes) of filler data to be placed in every object
	FillSize int `json:"fill_size"`
	// Count is the number of objects to seed the API server with.
	Count int `json:"count,omitempty"`
}

type Interact struct {
	// Proportions determines the ratio of different requests to make
	Proportions *Proportions `json:"proportions,omitempty"`
	// Parallelism determines how many parallel client connections to use
	Parallelism int `json:"parallelism,omitempty"`
	// Operations is the total number of client operations to execute
	Operations int `json:"operations,omitempty"`
}

type Proportions struct {
	Create int `json:"create,omitempty"`
	Get    int `json:"get,omitempty"`
	List   int `json:"list,omitempty"`
	Update int `json:"update,omitempty"`
	Delete int `json:"delete,omitempty"`
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
	if _, err := url.Parse(opts.apiserverUrl); err != nil {
		logrus.WithError(err).Fatal("invalid url")
	}

	raw, err := ioutil.ReadFile(opts.configFile)
	if err != nil {
		logrus.WithError(err).Fatal("could not read config file")
	}
	var config Config
	if err := json.Unmarshal(raw, &config); err != nil {
		logrus.WithError(err).Fatal("could not deserialize config")
	}

	if opts.apiserverFlags {
		storageServers := "localhost:2379"
		storageBackend := "etcd3"
		if config.Setup != nil && config.Setup.StorageBackend != "" {
			storageBackend = config.Setup.StorageBackend
			if storageBackend == "crdb" {
				u := url.URL{
					Scheme: "postgresql",
					User:   url.User("root"),
					Host:   storageServers,
					Path:   "defaultdb",
				}
				storageServers = u.String()
			}
		}
		var watchCache bool
		if config.Setup != nil {
			watchCache = config.Setup.WatchCacheEnabled
		}
		fmt.Printf("--storage-backend=%s --etcd-servers=%s --watch-cache=%t", storageBackend, storageServers, watchCache)
		return
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer func() {
		cancel()
	}()

	var im *interactionManager
	defer func() {
		if im == nil {
			logrus.Warn("could not dump data, interaction manager was nil")
			return
		}
		close(im.metricsChan)
		close(im.sizeChan)
		im.wg.Wait()
		raw, err := json.Marshal(map[string]interface{}{
			"sizeTimestamps": im.sizeTimestamps,
			"sizes":          im.sizes,
			"metrics":        im.metrics,
		})
		if err != nil {
			logrus.WithError(err).Errorf("failed to serialize events")
			return
		}
		if err := ioutil.WriteFile(opts.eventsFile, raw, 0666); err != nil {
			logrus.WithError(err).Errorf("failed to write events")
		}
		logrus.Infof("wrote events to %s, exiting", opts.eventsFile)
	}()

	if config.Seed != nil {
		logrus.Info("seeding the API server...")
		im, err = seed(ctx, config, opts.apiserverUrl, opts.token)
		logrus.Info("done seeding the API server...")
		if err != nil {
			logrus.WithError(err).Error("failed to seed the API server")
			return
		}
	}

	if config.Interact != nil {
		logrus.Info("interacting with the API server...")
		err = interact(ctx, config, im)
		logrus.Info("done interacting with the API server...")
		if err != nil {
			logrus.WithError(err).Error("failed to interact with the API server")
			return
		}
	}
}

func seed(ctx context.Context, config Config, apiserverUrl, token string) (*interactionManager, error) {
	clientSet, err := clientset.NewForConfig(&restclient.Config{
		Host:        apiserverUrl,
		BearerToken: token,
		TLSClientConfig: restclient.TLSClientConfig{
			Insecure: true,
		},
		QPS: -1,
	})
	if err != nil {
		return nil, fmt.Errorf("could not create client: %w", err)
	}

	logrus.Info("waiting for the API server to be ready...")
	var lastHealthContent string
	err = wait.PollImmediateWithContext(ctx, 100*time.Millisecond, 30*time.Second, func(ctx context.Context) (bool, error) {
		reqContext, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		result := clientSet.RESTClient().Get().AbsPath("/healthz").Do(reqContext)
		status := 0
		result.StatusCode(&status)
		if status == 200 {
			return true, nil
		}
		lastHealthBytes, _ := result.Raw()
		lastHealthContent = fmt.Sprintf("%d: %s", status, string(lastHealthBytes))
		return false, nil
	})
	if err != nil {
		return nil, fmt.Errorf("did not find API server ready, last response to /healthz: %s: %w", lastHealthContent, err)
	}

	logrus.Info("seeding the API server with objects...")
	ns, err := clientSet.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "benchmark"}}, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("could not create benchmark namespace: %w", err)
	}

	sa, err := clientSet.CoreV1().ServiceAccounts(ns.Name).Create(ctx, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "benchmark"}}, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("could not create service account: %w", err)
	}

	im := &interactionManager{
		client:       clientSet,
		r:            rand.New(rand.NewSource(1649262088392430503)),
		config:       config,
		ns:           ns,
		sa:           sa,
		existingLock: &sync.RWMutex{},
		existing:     []int{},
		metricsChan:  make(chan durationDatapoint, 100),
		sizeChan:     make(chan sizeDatapoint, 100),
		wg:           &sync.WaitGroup{},
		metrics: map[string][]time.Duration{
			"create": {},
			"get":    {},
			"list":   {},
			"update": {},
			"delete": {},
		},
	}
	im.consume(ctx)

	start := time.Now()
	for i := 0; i < config.Seed.Count; i++ {
		start = display(i, config.Seed.Count, start)
		if err := im.create(ctx); err != nil {
			return im, fmt.Errorf("could not create pod: %w", err)
		}
	}

	return im, nil
}

func display(i, max int, start time.Time) time.Time {
	if (i+1)%(max/10) == 0 {
		logrus.Infof("progress: %d/%d (%.0f%%); %s per ", i+1, max, 100*(float64(i+1)/float64(max)), time.Since(start)/time.Duration(max/10))
		return time.Now()
	}
	return start
}

func interact(ctx context.Context, config Config, im *interactionManager) error {
	wg := &sync.WaitGroup{}
	operations := make(chan struct{})
	for i := 0; i < config.Interact.Parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case _, ok := <-operations:
					if !ok {
						return
					}
					if err := im.do(ctx); err != nil {
						logrus.WithError(err).Error("failed to interact with the API server")
					}
				}
			}
		}()
	}

	start := time.Now()
	for i := 0; i < config.Interact.Operations; i++ {
		start = display(i, config.Interact.Operations, start)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case operations <- struct{}{}:
		}
	}
	close(operations)
	wg.Wait()
	return nil
}

type interactionManager struct {
	client clientset.Interface
	r      *rand.Rand
	config Config

	ns *corev1.Namespace
	sa *corev1.ServiceAccount

	existingLock *sync.RWMutex
	count        int
	existing     []int

	metricsChan chan durationDatapoint
	sizeChan    chan sizeDatapoint
	wg          *sync.WaitGroup

	metrics        map[string][]time.Duration
	sizeTimestamps []time.Time
	sizes          []int
}

type sizeDatapoint struct {
	timestamp time.Time
	value     int
}

type durationDatapoint struct {
	method   string
	duration time.Duration
}

func (i *interactionManager) consume(ctx context.Context) {
	i.wg.Add(1)
	go func() {
		defer i.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case metric, ok := <-i.metricsChan:
				if !ok {
					return
				}
				i.metrics[metric.method] = append(i.metrics[metric.method], metric.duration)

				var total int
				fields := logrus.Fields{}
				for k, v := range i.metrics {
					total += len(v)
					fields[k] = len(v)
				}
				if total%1000 == 0 {
					for k, v := range i.metrics {
						if len(v) == 0 {
							continue
						}
						var all time.Duration
						for _, i := range v {
							all += i
						}
						fields[fmt.Sprintf("%s-mean", k)] = all / time.Duration(len(v))
					}
					logrus.WithFields(fields).Infof("processed %d metrics", total)
				}
			}
		}
	}()
	i.wg.Add(1)
	go func() {
		defer i.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case datum, ok := <-i.sizeChan:
				if !ok {
					return
				}
				i.sizeTimestamps = append(i.sizeTimestamps, datum.timestamp.UTC())
				previousSize := 0
				if len(i.sizes) > 0 {
					previousSize = i.sizes[len(i.sizes)-1]
				}
				i.sizes = append(i.sizes, previousSize+datum.value)
			}
		}
	}()
}

type action uint8

const (
	actionCreate action = iota
	actionGet
	actionList
	actionUpdate
	actionDelete
)

func (i *interactionManager) do(ctx context.Context) error {
	prop := i.config.Interact.Proportions
	var actionOptions []action
	for actionType, count := range map[action]int{
		actionCreate: prop.Create,
		actionGet:    prop.Get,
		actionList:   prop.List,
		actionUpdate: prop.Update,
		actionDelete: prop.Delete,
	} {
		for j := 0; j < count; j++ {
			actionOptions = append(actionOptions, actionType)
		}
	}
	actions := map[action]func(context.Context) error{
		actionCreate: i.create,
		actionGet:    i.get,
		actionList:   i.list,
		actionUpdate: i.update,
		actionDelete: i.delete,
	}
	if err := actions[actionOptions[i.r.Intn(len(actionOptions))]](ctx); err != nil && !kerrors.IsNotFound(err) {
		return err
	}
	return nil
}

func (i *interactionManager) create(ctx context.Context) error {
	i.existingLock.Lock()
	key := i.count
	i.existing = append(i.existing, i.count)
	i.count++
	i.existingLock.Unlock() // technically racy if another client tries to interact with this before the call succeeds but we allow 404 so that's ok
	before := time.Now()
	_, err := i.client.CoreV1().Pods(i.ns.Name).Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "benchmark",
			Name:      fmt.Sprintf("pod-%d", key),
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:    "none",
				Image:   "quay.io/fedora/fedora:35",
				Command: []string{"sleep", "3600"},
				Env: []corev1.EnvVar{{
					Name:  "filler",
					Value: strings.Repeat("x", i.config.Seed.FillSize),
				}},
			}},
			ServiceAccountName:           i.sa.Name,
			AutomountServiceAccountToken: pointer.BoolPtr(false),
			NodeName:                     "node",
		},
	}, metav1.CreateOptions{})
	duration := time.Since(before)
	i.metricsChan <- durationDatapoint{method: "create", duration: duration}
	i.sizeChan <- sizeDatapoint{timestamp: before, value: i.config.Seed.FillSize}
	return err
}

func (i *interactionManager) get(ctx context.Context) error {
	i.existingLock.RLock()
	if len(i.existing) == 0 {
		i.existingLock.RUnlock()
		return i.create(ctx)
	}
	key := i.existing[i.r.Intn(len(i.existing))]
	i.existingLock.RUnlock()
	before := time.Now()
	_, err := i.client.CoreV1().Pods(i.ns.Name).Get(ctx, fmt.Sprintf("pod-%d", key), metav1.GetOptions{})
	duration := time.Since(before)
	i.metricsChan <- durationDatapoint{method: "get", duration: duration}
	return err
}

func (i *interactionManager) list(ctx context.Context) error {
	var duration time.Duration
	var cont string
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		before := time.Now()
		l, err := i.client.CoreV1().Pods(i.ns.Name).List(ctx, metav1.ListOptions{Limit: 5000, Continue: cont})
		duration = duration + time.Since(before)
		if err != nil {
			logrus.WithError(err).Warn("failed to read")
		}
		l.Items = nil // keep our memory use down
		cont = l.Continue
		if cont == "" {
			break
		}
	}
	i.metricsChan <- durationDatapoint{method: "list", duration: duration}
	return nil
}

func (i *interactionManager) update(ctx context.Context) error {
	i.existingLock.Lock()
	if len(i.existing) == 0 {
		i.existingLock.Unlock()
		return i.create(ctx)
	}
	idx := i.r.Intn(len(i.existing))
	key := i.existing[idx]
	counter := i.count
	i.existingLock.Unlock()
	before := time.Now()
	_, err := i.client.CoreV1().Pods(i.ns.Name).Patch(ctx, fmt.Sprintf("pod-%d", key), types.MergePatchType, []byte(fmt.Sprintf(`{"metadata":{"labels":{"counter":"%d"}}}`, counter)), metav1.PatchOptions{})
	duration := time.Since(before)
	i.metricsChan <- durationDatapoint{method: "update", duration: duration}
	return err
}

func (i *interactionManager) delete(ctx context.Context) error {
	i.existingLock.Lock()
	if len(i.existing) == 0 {
		i.existingLock.Unlock()
		return i.create(ctx)
	}
	idx := i.r.Intn(len(i.existing))
	key := i.existing[idx]
	i.existing = append(i.existing[:idx], i.existing[idx+1:]...)
	i.existingLock.Unlock()
	before := time.Now()
	err := i.client.CoreV1().Pods(i.ns.Name).Delete(ctx, fmt.Sprintf("pod-%d", key), metav1.DeleteOptions{})
	duration := time.Since(before)
	i.metricsChan <- durationDatapoint{method: "delete", duration: duration}
	i.sizeChan <- sizeDatapoint{timestamp: before, value: -i.config.Seed.FillSize}
	return err
}
