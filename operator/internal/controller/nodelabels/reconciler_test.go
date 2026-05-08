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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestReconcilerValuesRelistsMetadata(t *testing.T) {
	labelKey := "topology.grove.io/block"
	client := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(
			nodeMeta("node-a", map[string]string{labelKey: "fabric-a"}),
			nodeMeta("node-b", map[string]string{labelKey: "fabric-b"}),
			nodeMeta("node-c", map[string]string{}),
		).
		Build()

	store := NewReconciler(client)

	values, err := store.Values(context.Background(), labelKey)
	require.NoError(t, err)
	assert.Equal(t, []string{"fabric-a", "fabric-b"}, values)

	value, err := store.ValueForNode(context.Background(), labelKey, "node-a")
	require.NoError(t, err)
	assert.Equal(t, "fabric-a", value)
}

func TestReconcilerTracksMetadataChanges(t *testing.T) {
	labelKey := "topology.grove.io/block"
	ctx := context.Background()
	nodeA := nodeMeta("node-a", map[string]string{labelKey: "fabric-a"})
	nodeB := nodeMeta("node-b", map[string]string{labelKey: "fabric-b"})
	cl := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(nodeA, nodeB).
		Build()
	store := NewReconciler(cl)

	values, err := store.Values(ctx, labelKey)
	require.NoError(t, err)
	assert.Equal(t, []string{"fabric-a", "fabric-b"}, values)

	nodeA.SetLabels(map[string]string{labelKey: "fabric-c"})
	require.NoError(t, cl.Update(ctx, nodeA))
	_, err = store.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKey{Name: "node-a"}})
	require.NoError(t, err)

	values, err = store.Values(ctx, labelKey)
	require.NoError(t, err)
	assert.Equal(t, []string{"fabric-b", "fabric-c"}, values)

	require.NoError(t, cl.Delete(ctx, nodeA))
	_, err = store.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKey{Name: "node-a"}})
	require.NoError(t, err)

	values, err = store.Values(ctx, labelKey)
	require.NoError(t, err)
	assert.Equal(t, []string{"fabric-b"}, values)
}

func TestReconcilerTracksLabelWithNoInitialValues(t *testing.T) {
	labelKey := "topology.grove.io/block"
	ctx := context.Background()
	node := nodeMeta("node-a", map[string]string{})
	cl := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(node).
		Build()
	store := NewReconciler(cl)

	values, err := store.Values(ctx, labelKey)
	require.NoError(t, err)
	assert.Empty(t, values)

	node.SetLabels(map[string]string{labelKey: "fabric-a"})
	require.NoError(t, cl.Update(ctx, node))
	_, err = store.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKey{Name: "node-a"}})
	require.NoError(t, err)

	values, err = store.Values(ctx, labelKey)
	require.NoError(t, err)
	assert.Equal(t, []string{"fabric-a"}, values)
}

func TestReconcilerReconcileRemovesDeletedNode(t *testing.T) {
	labelKey := "topology.grove.io/block"
	ctx := context.Background()
	node := nodeMeta("node-a", map[string]string{labelKey: "fabric-a"})
	cl := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(node).
		Build()
	store := NewReconciler(cl)

	values, err := store.Values(ctx, labelKey)
	require.NoError(t, err)
	assert.Equal(t, []string{"fabric-a"}, values)

	require.NoError(t, cl.Delete(ctx, node))
	_, err = store.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKey{Name: "node-a"}})
	require.NoError(t, err)

	values, err = store.Values(ctx, labelKey)
	require.NoError(t, err)
	assert.Empty(t, values)
}

func TestReconcilerValueForNodeErrors(t *testing.T) {
	labelKey := "topology.grove.io/block"
	store := NewReconciler(fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(nodeMeta("node-a", map[string]string{})).
		Build())

	_, err := store.ValueForNode(context.Background(), labelKey, "missing-node")
	require.Error(t, err)
	assert.True(t, apierrors.IsNotFound(err))

	_, err = store.ValueForNode(context.Background(), "missing-label", "node-a")
	require.Error(t, err)
	assert.ErrorContains(t, err, "missing label on node")
}

func TestReconcilerRetriesAfterListFailure(t *testing.T) {
	labelKey := "topology.grove.io/block"
	ctx := context.Background()
	listCalls := 0
	cl := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(nodeMeta("node-a", map[string]string{labelKey: "fabric-a"})).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				listCalls++
				if listCalls == 1 {
					return errors.New("temporary list failure")
				}
				return cl.List(ctx, list, opts...)
			},
		}).
		Build()
	store := NewReconciler(cl)

	_, err := store.Values(ctx, labelKey)
	require.Error(t, err)

	values, err := store.Values(ctx, labelKey)
	require.NoError(t, err)
	assert.Equal(t, []string{"fabric-a"}, values)
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	scheme := runtime.NewScheme()
	scheme.AddKnownTypeWithName(corev1.SchemeGroupVersion.WithKind("Node"), &metav1.PartialObjectMetadata{})
	scheme.AddKnownTypeWithName(corev1.SchemeGroupVersion.WithKind("NodeList"), &metav1.PartialObjectMetadataList{})
	return scheme
}

func nodeMeta(name string, labels map[string]string) *metav1.PartialObjectMetadata {
	obj := &metav1.PartialObjectMetadata{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Node",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: labels,
		},
	}
	obj.SetGroupVersionKind(schema.GroupVersionKind{Version: "v1", Kind: "Node"})
	return obj
}
