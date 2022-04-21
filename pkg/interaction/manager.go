package interaction

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"path/filepath"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	clientset "k8s.io/client-go/kubernetes"
)

const serviceAccountName string = "benchmark"

func NewManager(ctx context.Context, fillSize, partitions int, client *clientset.Clientset) (*interactionManager, error) {
	logrus.Info("Waiting for the API server to be ready.")
	var lastHealthContent string
	err := wait.PollImmediateWithContext(ctx, 100*time.Millisecond, 30*time.Second, func(ctx context.Context) (bool, error) {
		reqContext, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		result := client.RESTClient().Get().AbsPath("/healthz").Do(reqContext)
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

	logrus.Info("Setting up namespace and service account.")
	var namespaces []string
	counts := map[string]int{}
	for i := 0; i < partitions; i++ {
		name := fmt.Sprintf("benchmark-%d", i)
		namespaces = append(namespaces, name)
		counts[name] = 0
		ns, err := client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}, metav1.CreateOptions{})
		if err != nil {
			return nil, fmt.Errorf("could not create benchmark namespace: %w", err)
		}

		_, err = client.CoreV1().ServiceAccounts(ns.Name).Create(ctx, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: serviceAccountName}}, metav1.CreateOptions{})
		if err != nil {
			return nil, fmt.Errorf("could not create service account: %w", err)
		}
	}

	im := &interactionManager{
		client:               client,
		r:                    rand.New(rand.NewSource(1649262088392430503)),
		fillSize:             fillSize,
		namespaces:           namespaces,
		count:                counts,
		timestampsLock:       &sync.RWMutex{},
		timestampsByRevision: map[string]time.Time{},
		existing:             []record{},
		existingLock:         &sync.RWMutex{},
		counterLock:          &sync.RWMutex{},
		metricsChan:          make(chan durationDatapoint, 100),
		sizeChan:             make(chan sizeDatapoint, 100),
		wg:                   &sync.WaitGroup{},
		metrics: map[string][]time.Duration{
			"create": {},
			"get":    {},
			"list":   {},
			"update": {},
			"delete": {},
			"watch":  {},
		},
		indexedMetrics: map[string]map[string][]time.Duration{
			"list": {},
		},
	}
	watchCancel := im.consume(ctx)
	im.watchCancel = watchCancel

	return im, nil
}

type interactionManager struct {
	client   clientset.Interface
	d delegate
	r        *rand.Rand
	fillSize int

	namespaces []string

	existingLock *sync.RWMutex
	count        map[string]int
	existing     []record

	timestampsLock       *sync.RWMutex
	timestampsByRevision map[string]time.Time

	metricsChan chan durationDatapoint
	sizeChan    chan sizeDatapoint
	wg          *sync.WaitGroup

	counterLock  *sync.RWMutex
	watchCounter int
	watchCancel func()

	metrics         map[string][]time.Duration
	indexedMetrics  map[string]map[string][]time.Duration
	sizeTimestamps  []time.Time
	sizes           []int
	watchTimestamps []time.Time
	watchCounts     []int
}

type record struct {
	namespace, name string
}

type sizeDatapoint struct {
	timestamp time.Time
	value     int
}

type durationDatapoint struct {
	method   string
	index    string
	duration time.Duration
}

func (i *interactionManager) consume(ctx context.Context) func() {
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
				if metric.index != "" {
					i.indexedMetrics[metric.method][metric.index] = append(i.indexedMetrics[metric.method][metric.index], metric.duration)
				}

				var total int
				fields := logrus.Fields{}
				for k, v := range i.metrics {
					total += len(v)
					fields[k] = len(v)
				}
				if total%10000 == 0 {
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
	watchCtx, watchCancel := context.WithCancel(ctx)
	i.wg.Add(1)
	go func() {
		ticker := time.Tick(500 * time.Millisecond)
		defer i.wg.Done()
		for {
			select {
			case <-watchCtx.Done():
				return
			case <-ticker:
				i.watchTimestamps = append(i.watchTimestamps, time.Now().UTC())
				i.counterLock.RLock()
				i.watchCounts = append(i.watchCounts, i.watchCounter)
				i.counterLock.RUnlock()
			}
		}
	}()
	return watchCancel
}

func (i *interactionManager) WriteOutput(outputDir string) error {
	close(i.metricsChan)
	close(i.sizeChan)
	i.watchCancel()
	i.wg.Wait()
	raw, err := json.Marshal(map[string]interface{}{
		"sizeTimestamps":  i.sizeTimestamps,
		"sizes":           i.sizes,
		"watchTimestamps": i.watchTimestamps,
		"watchCounts":     i.watchCounts,
		"metrics":         i.metrics,
		"indexedMetrics":  i.indexedMetrics,
	})
	if err != nil {
		return fmt.Errorf("failed to serialize interaction events and metrics: %w", err)
	}
	outputFile := filepath.Join(outputDir, "events.json")
	if err := ioutil.WriteFile(outputFile, raw, 0666); err != nil {
		return fmt.Errorf("failed to write interaction events and metrics: %w", err)
	}
	logrus.Infof("Wrote interaction events and metrics to %s.", filepath.Join(outputDir, "events.json"))
	return nil
}

func (i *interactionManager) create(ctx context.Context) error {
	return i.createWithNodeName(ctx, "node")
}

func (i *interactionManager) createWithNodeName(ctx context.Context, nodeName string) error {
	namespace := i.namespaces[i.r.Intn(len(i.namespaces))]
	i.existingLock.Lock()
	key := i.count[namespace]
	name := fmt.Sprintf("item-%d", key)
	i.existing = append(i.existing, record{namespace: namespace, name: name})
	i.count[namespace]++
	i.existingLock.Unlock() // technically racy if another client tries to interact with this before the call succeeds but we allow 404 so that's ok
	before := time.Now()
	rv, err := i.d.create(ctx, namespace, name)
	after := time.Now()
	duration := after.Sub(before)
	i.timestampsLock.Lock()
	i.timestampsByRevision[rv] = after
	i.timestampsLock.Unlock()
	select {
	case i.metricsChan <- durationDatapoint{method: "create", duration: duration}:
	case <-ctx.Done():
	}
	if err == nil {
		select {
		case i.sizeChan <- sizeDatapoint{timestamp: before, value: 1}:
		case <-ctx.Done():
		}
	}
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
	err := i.d.get(ctx, key.namespace, key.name)
	duration := time.Since(before)
	select {
	case i.metricsChan <- durationDatapoint{method: "get", duration: duration}:
	case <-ctx.Done():
	}
	return err
}

func (i *interactionManager) list(ctx context.Context) error {
	duration, err := i.doList(ctx, fields.Everything())
	select {
	case i.metricsChan <- durationDatapoint{method: "list", duration: duration}:
	case <-ctx.Done():
	}
	return err
}

func (i *interactionManager) doList(ctx context.Context, selector fields.Selector) (time.Duration, error) {
	namespace := i.namespaces[i.r.Intn(len(i.namespaces))]
	var duration time.Duration
	var cont string
	var err error
	for {
		select {
		case <-ctx.Done():
			return duration, ctx.Err()
		default:
		}
		before := time.Now()
		cont, err = i.d.list(ctx, namespace, cont, selector)
		duration = duration + time.Since(before)
		if err != nil {
			return duration, err
		}
		if cont == "" {
			break
		}
	}
	return duration, nil
}

func (i *interactionManager) listWithNodeName(ctx context.Context, nodeName string) error {
	duration, err := i.doList(ctx, fields.OneTermEqualSelector("spec.nodeName", nodeName))
	select {
	case i.metricsChan <- durationDatapoint{method: "list", index: nodeName, duration: duration}:
	case <-ctx.Done():
	}
	return err
}

func (i *interactionManager) update(ctx context.Context) error {
	i.existingLock.Lock()
	if len(i.existing) == 0 {
		i.existingLock.Unlock()
		return i.create(ctx)
	}
	idx := i.r.Intn(len(i.existing))
	key := i.existing[idx]
	counter := i.count[key.namespace]
	i.existingLock.Unlock()
	before := time.Now()
	rv, err := i.d.update(ctx, key.namespace, key.name, types.MergePatchType, []byte(fmt.Sprintf(`{"metadata":{"labels":{"counter":"%d"}}}`, counter)))
	after := time.Now()
	duration := after.Sub(before)
	i.timestampsLock.Lock()
	i.timestampsByRevision[rv] = after
	i.timestampsLock.Unlock()
	select {
	case i.metricsChan <- durationDatapoint{method: "update", duration: duration}:
	case <-ctx.Done():
	}
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
	err := i.d.delete(ctx, key.namespace, key.name)
	duration := time.Since(before)
	select {
	case i.metricsChan <- durationDatapoint{method: "delete", duration: duration}:
	case <-ctx.Done():
	}
	if err == nil {
		select {
		case i.sizeChan <- sizeDatapoint{timestamp: before, value: -1}:
		case <-ctx.Done():
		}
	}
	return err
}

func (i *interactionManager) watch(ctx context.Context) error {
	return i.watchNamespaced(ctx, i.namespaces[i.r.Intn(len(i.namespaces))])
}

func (i *interactionManager) watchNamespaced(ctx context.Context, namespace string) error {
	watcher, err := i.d.watch(ctx, namespace)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-watcher.ResultChan():
			recieved := time.Now()
			i.counterLock.Lock()
			i.watchCounter++
			i.counterLock.Unlock()
			if !ok {
				return nil
			}
			switch event.Type {
			case watch.Added, watch.Modified:
			case watch.Deleted, watch.Bookmark:
				continue // should not occur
			case watch.Error:
				if err, ok := event.Object.(*metav1.Status); ok && err != nil {
					return fmt.Errorf("watch failed: %w", &errors.StatusError{ErrStatus: *err})
				} else {
					return fmt.Errorf("watch failed: %T: %v", event.Object, event.Object)
				}
			}

			obj, ok := event.Object.(metav1.Object)
			if !ok {
				return fmt.Errorf("expected a metav1.Object in watch, got %T", event.Object)
			}
			i.timestampsLock.RLock()
			timestamp, ok := i.timestampsByRevision[obj.GetResourceVersion()]
			i.timestampsLock.RUnlock()
			if !ok {
				continue
			}

			select {
			case i.metricsChan <- durationDatapoint{method: "watch", duration: recieved.Sub(timestamp)}:
			case <-ctx.Done():
			}
		}
	}
}
