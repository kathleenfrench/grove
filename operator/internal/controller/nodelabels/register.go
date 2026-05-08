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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// NewReconciler creates a metadata-backed node label value store.
func NewReconciler(cl client.Client) *Reconciler {
	return &Reconciler{
		client: cl,
		values: make(map[string]sets.Set[string]),
	}
}

// RegisterWithManager registers the node label controller with the given controller manager.
func (s *Reconciler) RegisterWithManager(mgr manager.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("node-label-value-store").
		WithOptions(controller.Options{MaxConcurrentReconciles: 1}).
		WatchesMetadata(
			&corev1.Node{},
			handler.EnqueueRequestsFromMapFunc(func(context.Context, client.Object) []reconcile.Request {
				// coalesce updates to a single request so that multiple node updates are bucketed into one reconcile
				return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: "node-label-values"}}}
			}),
		).
		Complete(s)
}
