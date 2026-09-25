/*
Copyright 2026 The llm-d Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package podwatcher watches the processor StatefulSet via the Kubernetes
// API and notifies the reconciler when the set of ready pods stabilizes.
package podwatcher

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/go-logr/logr"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/logging"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

// PodEventHandler is called when the StatefulSet is stable (readyReplicas
// == spec.replicas) with the set of ready processor pod names.
type PodEventHandler func(liveProcessors map[string]bool)

// Watcher watches the processor StatefulSet. When readyReplicas equals
// spec.replicas (stable), it lists the ready pods and calls the handler
// with their names.
type Watcher struct {
	clientset        kubernetes.Interface
	namespace        string
	stsName          string
	podLabelSelector string
	handler          PodEventHandler
}

// New creates a new Watcher using in-cluster Kubernetes config.
// stsName is the processor StatefulSet name.
// podLabelSelector identifies processor pods for listing.
// The namespace is auto-detected from the service account mount.
func New(stsName, podLabelSelector string, handler PodEventHandler) (*Watcher, error) {
	restCfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get in-cluster config: %w", err)
	}

	cs, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	ns, err := detectNamespace()
	if err != nil {
		return nil, fmt.Errorf("failed to detect namespace: %w", err)
	}

	return &Watcher{
		clientset:        cs,
		namespace:        ns,
		stsName:          stsName,
		podLabelSelector: podLabelSelector,
		handler:          handler,
	}, nil
}

// Run watches the processor StatefulSet and blocks until the context is
// cancelled. On each StatefulSet update where readyReplicas == replicas,
// it lists the ready pods and calls the handler.
func (w *Watcher) Run(ctx context.Context) error {
	logger := logr.FromContextOrDiscard(ctx)

	factory := informers.NewSharedInformerFactoryWithOptions(
		w.clientset,
		0,
		informers.WithNamespace(w.namespace),
		informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			opts.FieldSelector = "metadata.name=" + w.stsName
		}),
	)

	stsInformer := factory.Apps().V1().StatefulSets().Informer()
	if _, err := stsInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			w.onStatefulSetChange(ctx, logger, obj)
		},
		UpdateFunc: func(_, newObj interface{}) {
			w.onStatefulSetChange(ctx, logger, newObj)
		},
	}); err != nil {
		return fmt.Errorf("failed to add event handler: %w", err)
	}

	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), stsInformer.HasSynced) {
		return fmt.Errorf("failed to sync StatefulSet informer cache")
	}

	logger.Info("Pod watcher started",
		"namespace", w.namespace,
		"statefulSet", w.stsName,
		"podLabelSelector", w.podLabelSelector)

	<-ctx.Done()
	return ctx.Err()
}

func (w *Watcher) onStatefulSetChange(ctx context.Context, logger logr.Logger, obj interface{}) {
	sts, ok := obj.(*appsv1.StatefulSet)
	if !ok || sts.Name != w.stsName {
		return
	}

	desired := int32(1)
	if sts.Spec.Replicas != nil {
		desired = *sts.Spec.Replicas
	}

	if sts.Status.ReadyReplicas != desired {
		logger.V(logging.INFO).Info("StatefulSet not stable",
			"ready", sts.Status.ReadyReplicas, "desired", desired)
		return
	}

	live, err := w.LivePods(ctx)
	if err != nil {
		logger.Error(err, "Failed to list processor pods")
		return
	}

	logger.Info("StatefulSet stable, updating live processors",
		"ready", len(live), "desired", desired)
	w.handler(live)
}

// LivePods returns the set of currently-Ready processor pod names. The
// reconciler calls it each cycle to refresh its live set from actual pod
// readiness, independent of the StatefulSet's stable-state gating, so a
// freshly-Ready replica is never treated as a dead owner.
func (w *Watcher) LivePods(ctx context.Context) (map[string]bool, error) {
	podList, err := w.clientset.CoreV1().Pods(w.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: w.podLabelSelector,
	})
	if err != nil {
		return nil, err
	}

	live := make(map[string]bool, len(podList.Items))
	for i := range podList.Items {
		if isPodReady(&podList.Items[i]) {
			live[podList.Items[i].Name] = true
		}
	}
	return live, nil
}

func isPodReady(pod *corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func detectNamespace() (string, error) {
	data, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}
