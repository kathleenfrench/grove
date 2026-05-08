// /*
// Copyright 2026 The Grove Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// */

package nodelabels

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	nodeGVK     = corev1.SchemeGroupVersion.WithKind("Node")
	nodeListGVK = corev1.SchemeGroupVersion.WithKind("NodeList")
)

// Reconciler watches Node metadata and keeps unique values for
// dynamically registered label keys.
type Reconciler struct {
	client client.Client

	mu sync.RWMutex
	// A key exists in values only after that label has been requested. A non-nil
	// empty set means the label is tracked but no Nodes currently expose it.
	values map[string]sets.Set[string]
}

// Reconcile reconciles a Node resource.
func (s *Reconciler) Reconcile(ctx context.Context, _ ctrl.Request) (ctrl.Result, error) {
	return ctrl.Result{}, s.rebuild(ctx)
}

// Values starts tracking a label key and returns its known values.
func (s *Reconciler) Values(ctx context.Context, labelKey string) ([]string, error) {
	if labelKey == "" {
		return nil, fmt.Errorf("label key must not be empty")
	}

	s.mu.RLock()
	values, ok := s.values[labelKey]
	s.mu.RUnlock()

	if ok {
		return sets.List(values), nil
	}

	s.mu.Lock()
	s.values[labelKey] = sets.New[string]()
	s.mu.Unlock()

	if err := s.rebuild(ctx); err != nil {
		return nil, fmt.Errorf("failed to list node metadata for label %q: %w", labelKey, err)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return sets.List(s.values[labelKey]), nil
}

// ValueForNode fetches the current metadata for a Node and returns the requested label value.
func (s *Reconciler) ValueForNode(ctx context.Context, labelKey, nodeName string) (string, error) {
	if labelKey == "" {
		return "", fmt.Errorf("label key must not be empty")
	}
	if nodeName == "" {
		return "", fmt.Errorf("node name must not be empty")
	}

	nodeMeta := &metav1.PartialObjectMetadata{}
	nodeMeta.SetName(nodeName)
	nodeMeta.SetGroupVersionKind(nodeGVK)

	if err := s.client.Get(ctx, client.ObjectKey{Name: nodeName}, nodeMeta); err != nil {
		return "", err
	}

	if value := nodeMeta.Labels[labelKey]; value != "" {
		return value, nil
	}

	return "", fmt.Errorf("missing label on node")
}

func (s *Reconciler) rebuild(ctx context.Context) error {
	s.mu.RLock()
	labels := slices.Collect(maps.Keys(s.values))
	s.mu.RUnlock()

	if len(labels) == 0 {
		return nil
	}

	nodeList := &metav1.PartialObjectMetadataList{}
	nodeList.SetGroupVersionKind(nodeListGVK)

	values := make(map[string]sets.Set[string], len(labels))
	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.values = values
	}()

	if err := s.client.List(ctx, nodeList); err != nil {
		return err
	}

	for _, labelKey := range labels {
		values[labelKey] = sets.New[string]()
	}
	for _, node := range nodeList.Items {
		for labelKey := range values {
			if value := node.Labels[labelKey]; value != "" {
				values[labelKey].Insert(value)
			}
		}
	}

	return nil
}
