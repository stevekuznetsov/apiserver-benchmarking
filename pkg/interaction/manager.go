package interaction

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/utils/pointer"
)

func NewManager(ctx context.Context, fillSize int, client *clientset.Clientset) (*interactionManager, error) {
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
	ns, err := client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "benchmark"}}, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("could not create benchmark namespace: %w", err)
	}

	sa, err := client.CoreV1().ServiceAccounts(ns.Name).Create(ctx, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "benchmark"}}, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("could not create service account: %w", err)
	}

	im := &interactionManager{
		client:       client,
		r:            rand.New(rand.NewSource(1649262088392430503)),
		fillSize:     fillSize,
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
		indexedMetrics: map[string]map[string][]time.Duration{
			"list": {},
		},
	}
	im.consume(ctx)

	return im, nil
}

type interactionManager struct {
	client   clientset.Interface
	r        *rand.Rand
	fillSize int

	ns *corev1.Namespace
	sa *corev1.ServiceAccount

	existingLock *sync.RWMutex
	count        int
	existing     []int

	metricsChan chan durationDatapoint
	sizeChan    chan sizeDatapoint
	wg          *sync.WaitGroup

	metrics        map[string][]time.Duration
	indexedMetrics map[string]map[string][]time.Duration
	sizeTimestamps []time.Time
	sizes          []int
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
}

func (i *interactionManager) WriteOutput(outputDir string) error {
	close(i.metricsChan)
	close(i.sizeChan)
	i.wg.Wait()
	raw, err := json.Marshal(map[string]interface{}{
		"sizeTimestamps": i.sizeTimestamps,
		"sizes":          i.sizes,
		"metrics":        i.metrics,
		"indexedMetrics": i.indexedMetrics,
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
					Value: strings.Repeat("x", i.fillSize),
				}},
			}},
			ServiceAccountName:           i.sa.Name,
			AutomountServiceAccountToken: pointer.BoolPtr(false),
			NodeName:                     nodeName,
		},
	}, metav1.CreateOptions{})
	duration := time.Since(before)
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
	_, err := i.client.CoreV1().Pods(i.ns.Name).Get(ctx, fmt.Sprintf("pod-%d", key), metav1.GetOptions{})
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
	var duration time.Duration
	var cont string
	for {
		select {
		case <-ctx.Done():
			return duration, ctx.Err()
		default:
		}
		before := time.Now()
		l, err := i.client.CoreV1().Pods(i.ns.Name).List(ctx, metav1.ListOptions{TimeoutSeconds: pointer.Int64(600), Continue: cont, FieldSelector: selector.String()})
		duration = duration + time.Since(before)
		if err != nil {
			return duration, err
		}
		l.Items = nil // keep our memory use down
		cont = l.Continue
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
	counter := i.count
	i.existingLock.Unlock()
	before := time.Now()
	_, err := i.client.CoreV1().Pods(i.ns.Name).Patch(ctx, fmt.Sprintf("pod-%d", key), types.MergePatchType, []byte(fmt.Sprintf(`{"metadata":{"labels":{"counter":"%d"}}}`, counter)), metav1.PatchOptions{})
	duration := time.Since(before)
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
	err := i.client.CoreV1().Pods(i.ns.Name).Delete(ctx, fmt.Sprintf("pod-%d", key), metav1.DeleteOptions{})
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
