// /*
// Copyright 2026 The Grove Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// */

package lpx

import (
	"context"
	"testing"

	configv1alpha1 "github.com/ai-dynamo/grove/operator/api/config/v1alpha1"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"

	groveschedulerv1alpha1 "github.com/ai-dynamo/grove/scheduler/api/core/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestBackendPreparePod(t *testing.T) {
	backend := New(nil, configv1alpha1.SchedulerProfile{Name: configv1alpha1.SchedulerNameLPX})
	pod := testutils.NewPodBuilder("test-pod", "default").
		WithSchedulerName("default-scheduler").
		Build()
	pod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: "grove.io/podgang-pending-creation"}}
	pod.Spec.ResourceClaims = []corev1.PodResourceClaim{{
		Name:              "partition",
		ResourceClaimName: ptr.To("model-partition-000"),
	}}

	backend.PreparePod(pod)

	assert.Equal(t, string(configv1alpha1.SchedulerNameLPX), pod.Spec.SchedulerName)
	require.Len(t, pod.Spec.SchedulingGates, 1)
	assert.Equal(t, "grove.io/podgang-pending-creation", pod.Spec.SchedulingGates[0].Name)
	require.Len(t, pod.Spec.ResourceClaims, 1)
	assert.Equal(t, "model-partition-000", *pod.Spec.ResourceClaims[0].ResourceClaimName)
}

func TestBackendValidatePodCliqueSet(t *testing.T) {
	tests := []struct {
		name      string
		mutatePCS func(*grovecorev1alpha1.PodCliqueSet)
		wantError bool
	}{
		{
			name:      "no Grove topology constraints",
			mutatePCS: func(_ *grovecorev1alpha1.PodCliqueSet) {},
		},
		{
			name: "PodCliqueSet topology constraint",
			mutatePCS: func(pcs *grovecorev1alpha1.PodCliqueSet) {
				pcs.Spec.Template.TopologyConstraint = &grovecorev1alpha1.TopologyConstraint{}
			},
			wantError: true,
		},
		{
			name: "PodClique topology constraint",
			mutatePCS: func(pcs *grovecorev1alpha1.PodCliqueSet) {
				pcs.Spec.Template.Cliques[0].TopologyConstraint = &grovecorev1alpha1.TopologyConstraint{}
			},
			wantError: true,
		},
		{
			name: "PodCliqueScalingGroup topology constraint",
			mutatePCS: func(pcs *grovecorev1alpha1.PodCliqueSet) {
				pcs.Spec.Template.PodCliqueScalingGroupConfigs = []grovecorev1alpha1.PodCliqueScalingGroupConfig{{
					Name:               "workers",
					TopologyConstraint: &grovecorev1alpha1.TopologyConstraint{},
				}}
			},
			wantError: true,
		},
	}

	backend := New(nil, configv1alpha1.SchedulerProfile{Name: configv1alpha1.SchedulerNameLPX})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pcs := testutils.NewPodCliqueSetBuilder("test-pcs", "default", types.UID("test-uid")).
				WithPodCliqueTemplateSpec(
					testutils.NewPodCliqueTemplateSpecBuilder("worker").
						WithRoleName("worker").
						WithReplicas(1).
						Build(),
				).
				Build()
			tt.mutatePCS(pcs)

			err := backend.ValidatePodCliqueSet(context.Background(), pcs)

			if tt.wantError {
				require.ErrorIs(t, err, errTopologyConstraintsUnsupported)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestResolveCandidatePoolSelection(t *testing.T) {
	selected := candidatePod("gpu-a", "pod-a")
	surplus := candidatePod("gpu-b", "pod-b")
	allocation := candidateAllocation("bound")
	cl := testutils.CreateDefaultFakeClient([]client.Object{allocation})
	backend := New(cl, configv1alpha1.SchedulerProfile{
		Name: configv1alpha1.SchedulerNameLPX,
	}).(*schedulerBackend)

	selection, err := backend.ResolveCandidatePoolSelection(
		context.Background(),
		candidatePodGang("pod-gang-uid"),
		candidatePodClique(),
		[]*corev1.Pod{selected, surplus},
	)

	require.NoError(t, err)
	require.NotNil(t, selection)
	assert.Equal(t, allocation.GetUID(), selection.AllocationUID)
	assert.Equal(t, allocation.GetGeneration(), selection.AllocationGeneration)
	assert.Equal(t, "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", selection.PlanDigest)
	assert.Equal(t, []types.UID{"pod-a"}, selection.PodUIDs)
}

func TestResolveCandidatePoolSelectionRequiresDurablePhase(t *testing.T) {
	tests := []struct {
		phase        string
		wantSelected bool
	}{
		{phase: "planned"},
		{phase: "reserving"},
		{phase: "binding"},
		{phase: "bound", wantSelected: true},
		{phase: "degraded", wantSelected: true},
		{phase: "releasing"},
		{phase: "released"},
		{phase: "unsupported"},
		{phase: "failed"},
	}

	for _, tt := range tests {
		t.Run(tt.phase, func(t *testing.T) {
			allocation := candidateAllocation(tt.phase)
			cl := testutils.CreateDefaultFakeClient([]client.Object{allocation})
			backend := New(cl, configv1alpha1.SchedulerProfile{
				Name: configv1alpha1.SchedulerNameLPX,
			}).(*schedulerBackend)

			selection, err := backend.ResolveCandidatePoolSelection(
				context.Background(),
				candidatePodGang("pod-gang-uid"),
				candidatePodClique(),
				[]*corev1.Pod{candidatePod("gpu-a", "pod-a")},
			)

			require.NoError(t, err)
			if tt.wantSelected {
				require.NotNil(t, selection)
				assert.Equal(t, []types.UID{"pod-a"}, selection.PodUIDs)
				return
			}
			assert.Nil(t, selection)
		})
	}
}

func TestResolveCandidatePoolSelectionWaitsForAllocationStatus(t *testing.T) {
	allocation := candidateAllocation("bound")
	unstructured.RemoveNestedField(allocation.Object, "status")
	cl := testutils.CreateDefaultFakeClient([]client.Object{allocation})
	backend := New(cl, configv1alpha1.SchedulerProfile{
		Name: configv1alpha1.SchedulerNameLPX,
	}).(*schedulerBackend)

	selection, err := backend.ResolveCandidatePoolSelection(
		context.Background(),
		candidatePodGang("pod-gang-uid"),
		candidatePodClique(),
		[]*corev1.Pod{candidatePod("gpu-a", "pod-a")},
	)

	require.NoError(t, err)
	assert.Nil(t, selection)
}

func TestResolveCandidatePoolSelectionRequiresExactPodGangUID(t *testing.T) {
	allocation := candidateAllocation("bound")
	cl := testutils.CreateDefaultFakeClient([]client.Object{allocation})
	backend := New(cl, configv1alpha1.SchedulerProfile{
		Name: configv1alpha1.SchedulerNameLPX,
	}).(*schedulerBackend)

	selection, err := backend.ResolveCandidatePoolSelection(
		context.Background(),
		candidatePodGang("replacement-pod-gang-uid"),
		candidatePodClique(),
		[]*corev1.Pod{candidatePod("gpu-a", "pod-a")},
	)

	require.NoError(t, err)
	assert.Nil(t, selection)
}

func TestResolveCandidatePoolSelectionRejectsMissingSelectedCandidate(t *testing.T) {
	allocation := candidateAllocation("bound")
	cl := testutils.CreateDefaultFakeClient([]client.Object{allocation})
	backend := New(cl, configv1alpha1.SchedulerProfile{
		Name: configv1alpha1.SchedulerNameLPX,
	}).(*schedulerBackend)

	_, err := backend.ResolveCandidatePoolSelection(
		context.Background(),
		candidatePodGang("pod-gang-uid"),
		candidatePodClique(),
		[]*corev1.Pod{candidatePod("gpu-b", "pod-b")},
	)

	require.ErrorContains(t, err, "candidate Pod default/gpu-a UID \"pod-a\"")
}

func TestResolveCandidatePoolSelectionRejectsUIDNameMismatch(t *testing.T) {
	allocation := candidateAllocation("bound")
	cl := testutils.CreateDefaultFakeClient([]client.Object{allocation})
	backend := New(cl, configv1alpha1.SchedulerProfile{
		Name: configv1alpha1.SchedulerNameLPX,
	}).(*schedulerBackend)

	_, err := backend.ResolveCandidatePoolSelection(
		context.Background(),
		candidatePodGang("pod-gang-uid"),
		candidatePodClique(),
		[]*corev1.Pod{candidatePod("different-name", "pod-a")},
	)

	require.ErrorContains(t, err, "current Pod is default/different-name")
}

func TestResolveCandidatePoolSelectionRejectsEmptyCandidateSelection(t *testing.T) {
	allocation := candidateAllocation("bound")
	require.NoError(t, unstructured.SetNestedSlice(
		allocation.Object,
		[]interface{}{candidatePlacement("lpu-a", "lpu-pod")},
		"spec",
		"consumerPlacements",
	))
	cl := testutils.CreateDefaultFakeClient([]client.Object{allocation})
	backend := New(cl, configv1alpha1.SchedulerProfile{
		Name: configv1alpha1.SchedulerNameLPX,
	}).(*schedulerBackend)

	_, err := backend.ResolveCandidatePoolSelection(
		context.Background(),
		candidatePodGang("pod-gang-uid"),
		candidatePodClique(),
		[]*corev1.Pod{candidatePod("gpu-a", "pod-a")},
	)

	require.ErrorContains(t, err, "has no selected candidate Pods")
}

func candidateAllocation(phase string) *unstructured.Unstructured {
	allocation := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": allocationAPIGroup + "/" + allocationAPIVersion,
			"kind":       allocationKind,
			"metadata": map[string]interface{}{
				"name":       allocationName,
				"uid":        "allocation-uid",
				"generation": int64(4),
			},
			"spec": map[string]interface{}{
				"planDigest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				"podGangRef": map[string]interface{}{
					"name":      "model-0",
					"namespace": "default",
					"uid":       "pod-gang-uid",
				},
				"consumerPlacements": []interface{}{
					candidatePlacement("gpu-a", "pod-a"),
				},
			},
			"status": map[string]interface{}{
				"phase": phase,
			},
		},
	}
	allocation.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   allocationAPIGroup,
		Version: allocationAPIVersion,
		Kind:    allocationKind,
	})
	return allocation
}

func candidatePlacement(podName string, podUID string) map[string]interface{} {
	return map[string]interface{}{
		"podRef": map[string]interface{}{
			"name":      podName,
			"namespace": "default",
		},
		"podUid": podUID,
	}
}

func candidatePodGang(uid types.UID) *groveschedulerv1alpha1.PodGang {
	return &groveschedulerv1alpha1.PodGang{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "model-0",
			Namespace: "default",
			UID:       uid,
		},
		Spec: groveschedulerv1alpha1.PodGangSpec{
			PodGroups: []groveschedulerv1alpha1.PodGroup{{
				Name: "model-0-gpu",
				PodReferences: []groveschedulerv1alpha1.NamespacedName{
					{Namespace: "default", Name: "gpu-a"},
					{Namespace: "default", Name: "gpu-b"},
				},
			}},
		},
	}
}

func candidatePodClique() *grovecorev1alpha1.PodClique {
	return &grovecorev1alpha1.PodClique{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "model-0-gpu",
			Namespace: "default",
		},
	}
}

func candidatePod(name string, uid types.UID) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			UID:       uid,
		},
	}
}
