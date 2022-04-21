package interaction

import (
	"context"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/utils/pointer"
)

type delegate interface {
	create(ctx context.Context, namespace, name string) (revision string, err error)
	get(ctx context.Context, namespace, name string) error
	list(ctx context.Context, namespace, cont string, selector fields.Selector) (nextCont string, err error)
	update(ctx context.Context, namespace, name string, mergeType types.PatchType, patch []byte) (revision string, err error)
	delete(ctx context.Context, namespace, name string) error
	watch(ctx context.Context, namespace string) (watcher watch.Interface, err error)
}

type podDelegate struct {
	fillSize int
	client   clientset.Interface

	nodeNames chan int
}

func (p *podDelegate) create(ctx context.Context, namespace, name string) (revision string, err error) {
	obj, err := p.client.CoreV1().Pods(namespace).Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:    "none",
				Image:   "quay.io/fedora/fedora:35",
				Command: []string{"sleep", "3600"},
				Env: []corev1.EnvVar{{
					Name:  "filler",
					Value: strings.Repeat("x", p.fillSize),
				}},
			}},
			ServiceAccountName:           serviceAccountName,
			AutomountServiceAccountToken: pointer.BoolPtr(false),
			NodeName:                     strconv.Itoa(<-p.nodeNames),
		},
	}, metav1.CreateOptions{})
	if err != nil {
		return "", err
	}
	return obj.ResourceVersion, nil
}

func (p *podDelegate) get(ctx context.Context, namespace, name string) error {
	_, err := p.client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
	return err
}

func (p *podDelegate) list(ctx context.Context, namespace, cont string, selector fields.Selector) (nextCont string, err error) {
	l, err := p.client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{TimeoutSeconds: pointer.Int64(600), Continue: cont, FieldSelector: selector.String()})
	if err != nil {
		return "", err
	}
	l.Items = nil // keep our memory use down
	return l.Continue, nil
}

func (p *podDelegate) update(ctx context.Context, namespace, name string, mergeType types.PatchType, patch []byte) (revision string, err error) {
	obj, err := p.client.CoreV1().Pods(namespace).Patch(ctx, name, mergeType, patch, metav1.PatchOptions{})
	if err != nil {
		return "", err
	}
	return obj.ResourceVersion, nil
}

func (p *podDelegate) delete(ctx context.Context, namespace, name string) error {
	return p.client.CoreV1().Pods(namespace).Delete(ctx, name, metav1.DeleteOptions{})
}

func (p *podDelegate) watch(ctx context.Context, namespace string) (watcher watch.Interface, err error) {
	return p.client.CoreV1().Pods(namespace).Watch(ctx, metav1.ListOptions{})
}

var _ delegate = &podDelegate{}

type configmapDelegate struct {
	fillSize int
	client   clientset.Interface
}

func (c *configmapDelegate) create(ctx context.Context, namespace, name string) (revision string, err error) {
	obj, err := c.client.CoreV1().ConfigMaps(namespace).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
		Data: map[string]string{
			"filler": strings.Repeat("x", c.fillSize),
		},
	}, metav1.CreateOptions{})
	if err != nil {
		return "", err
	}
	return obj.ResourceVersion, nil
}

func (c *configmapDelegate) get(ctx context.Context, namespace, name string) error {
	_, err := c.client.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
	return err
}

func (c *configmapDelegate) list(ctx context.Context, namespace, cont string, selector fields.Selector) (nextCont string, err error) {
	l, err := c.client.CoreV1().ConfigMaps(namespace).List(ctx, metav1.ListOptions{TimeoutSeconds: pointer.Int64(600), Continue: cont, FieldSelector: selector.String()})
	if err != nil {
		return "", err
	}
	l.Items = nil // keep our memory use down
	return l.Continue, nil
}

func (c *configmapDelegate) update(ctx context.Context, namespace, name string, mergeType types.PatchType, patch []byte) (revision string, err error) {
	obj, err := c.client.CoreV1().ConfigMaps(namespace).Patch(ctx, name, mergeType, patch, metav1.PatchOptions{})
	if err != nil {
		return "", err
	}
	return obj.ResourceVersion, nil
}

func (c *configmapDelegate) delete(ctx context.Context, namespace, name string) error {
	return c.client.CoreV1().ConfigMaps(namespace).Delete(ctx, name, metav1.DeleteOptions{})
}

func (c *configmapDelegate) watch(ctx context.Context, namespace string) (watcher watch.Interface, err error) {
	return c.client.CoreV1().ConfigMaps(namespace).Watch(ctx, metav1.ListOptions{})
}

var _ delegate = &configmapDelegate{}