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
	lpxv1alpha1 "github.com/nvidia-lpu/lpx-scheduler/api/go/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

	require.NoError(t, backend.PreparePod(pod))

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

func TestBackendValidateCandidatePoolTopology(t *testing.T) {
	pcs := testutils.NewPodCliqueSetBuilder("test-pcs", "default", "pcs-uid").
		WithPodCliqueTemplateSpec(
			testutils.NewPodCliqueTemplateSpecBuilder("candidate").
				WithReplicas(1).
				Build(),
		).
		Build()
	pcs.Spec.Template.Cliques[0].Spec.Affinity = &grovecorev1alpha1.PodCliqueAffinity{
		TopologyAffinity: &grovecorev1alpha1.TopologyAffinity{
			TopologyName: "fabric",
			Domain:       "fabric-pod",
			CliqueNames:  []string{"conductor"},
		},
	}
	binding := &grovecorev1alpha1.ClusterTopologyBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "fabric"},
		Spec: grovecorev1alpha1.ClusterTopologyBindingSpec{
			Levels: []grovecorev1alpha1.TopologyLevel{{
				Domain: "fabric-pod",
				Key:    lpuFabricTopologyLabel,
			}},
		},
	}

	backend := New(
		testutils.CreateDefaultFakeClient([]client.Object{binding}),
		configv1alpha1.SchedulerProfile{Name: configv1alpha1.SchedulerNameLPX},
	)
	require.NoError(t, backend.ValidatePodCliqueSet(context.Background(), pcs))

	binding.Spec.Levels[0].Key = "topology.example.com/fabric"
	backend = New(
		testutils.CreateDefaultFakeClient([]client.Object{binding}),
		configv1alpha1.SchedulerProfile{Name: configv1alpha1.SchedulerNameLPX},
	)
	require.ErrorContains(
		t,
		backend.ValidatePodCliqueSet(context.Background(), pcs),
		lpuFabricTopologyLabel,
	)
}

func TestResolveCandidatePoolSelectionUsesCommittedActivePlanDomains(t *testing.T) {
	for _, phase := range []lpxv1alpha1.RequestPhase{
		lpxv1alpha1.RequestPhaseBound,
		lpxv1alpha1.RequestPhaseDegraded,
	} {
		t.Run(string(phase), func(t *testing.T) {
			request := candidateLPXRequest("request", "request-uid", phase)
			backend := candidateBackend(t, request)

			selection, err := backend.ResolveCandidatePoolSelection(
				context.Background(),
				candidatePodGang(),
				candidatePodClique(),
			)

			require.NoError(t, err)
			require.NotNil(t, selection)
			assert.True(t, selection.SourcePlanObserved)
			assert.True(t, selection.Eligible)
			assert.Equal(t, types.UID("request-uid"), selection.SourceUID)
			assert.Equal(t, int64(7), selection.SourceRevision)
			assert.Equal(t, "plan-digest", selection.SourceDigest)
			assert.Equal(t, []string{"fabric-a", "fabric-c"}, selection.SelectedDomains)
		})
	}
}

func TestResolveCandidatePoolSelectionRequiresDurablePhase(t *testing.T) {
	for _, phase := range []lpxv1alpha1.RequestPhase{
		lpxv1alpha1.RequestPhasePending,
		lpxv1alpha1.RequestPhasePlanned,
		lpxv1alpha1.RequestPhaseReserving,
		lpxv1alpha1.RequestPhaseBinding,
		lpxv1alpha1.RequestPhaseReleasing,
		lpxv1alpha1.RequestPhaseReleased,
	} {
		t.Run(string(phase), func(t *testing.T) {
			backend := candidateBackend(t, candidateLPXRequest("request", "request-uid", phase))

			selection, err := backend.ResolveCandidatePoolSelection(
				context.Background(),
				candidatePodGang(),
				candidatePodClique(),
			)

			require.NoError(t, err)
			require.NotNil(t, selection)
			assert.False(t, selection.Eligible)
			assert.True(t, selection.SourcePlanObserved)
			assert.Equal(t, types.UID("request-uid"), selection.SourceUID)
			assert.Equal(t, int64(7), selection.SourceRevision)
			assert.Equal(t, "plan-digest", selection.SourceDigest)
			assert.Equal(t, []string{"fabric-a", "fabric-c"}, selection.SelectedDomains)
		})
	}
}

func TestResolveCandidatePoolSelectionObservesLiveReplacementBeforePlan(t *testing.T) {
	request := candidateLPXRequest(
		"replacement",
		"replacement-request-uid",
		lpxv1alpha1.RequestPhasePending,
	)
	request.Status = nil
	backend := candidateBackend(t, request)

	observation, err := backend.ResolveCandidatePoolSelection(
		context.Background(),
		candidatePodGang(),
		candidatePodClique(),
	)

	require.NoError(t, err)
	require.NotNil(t, observation)
	assert.Equal(t, types.UID("replacement-request-uid"), observation.SourceUID)
	assert.False(t, observation.SourcePlanObserved)
	assert.False(t, observation.Eligible)
}

func TestResolveCandidatePoolSelectionFailsClosedOnReplacedReferences(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*lpxv1alpha1.LPUPipelineRequest)
		want   string
	}{
		{
			name: "replaced PodGang UID",
			mutate: func(request *lpxv1alpha1.LPUPipelineRequest) {
				request.Status.ActivePlan.Placement.PodGangRef.UID = "old-pod-gang-uid"
			},
			want: "contradictory active-plan PodGang reference",
		},
		{
			name: "replaced PodClique UID",
			mutate: func(request *lpxv1alpha1.LPUPipelineRequest) {
				request.Spec.CyborgPodCliqueRef.UID = "old-candidate-uid"
			},
			want: "contradictory candidate PodClique reference",
		},
		{
			name: "different spec PodGang",
			mutate: func(request *lpxv1alpha1.LPUPipelineRequest) {
				request.Spec.PodGangRef.Name = "other-pod-gang"
			},
			want: "contradictory spec PodGang reference",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := candidateLPXRequest(
				"request",
				"request-uid",
				lpxv1alpha1.RequestPhaseBound,
			)
			test.mutate(request)
			backend := candidateBackend(t, request)

			_, err := backend.ResolveCandidatePoolSelection(
				context.Background(),
				candidatePodGang(),
				candidatePodClique(),
			)

			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestResolveCandidatePoolSelectionHandlesForeignAndAmbiguousRequests(t *testing.T) {
	foreign := candidateLPXRequest("foreign", "foreign-uid", lpxv1alpha1.RequestPhaseBound)
	foreign.Spec.PodGangRef.Name = "foreign-gang"
	foreign.Status.ActivePlan.Placement.PodGangRef = lpxv1alpha1.ObjectReference{
		Name:      "foreign-gang",
		Namespace: "default",
		UID:       "foreign-gang-uid",
	}
	foreign.Spec.CyborgPodCliqueRef = &lpxv1alpha1.CyborgPodCliqueReference{
		Name: "foreign-candidate",
		UID:  "foreign-candidate-uid",
	}
	exact := candidateLPXRequest("exact", "exact-uid", lpxv1alpha1.RequestPhaseBound)
	backend := candidateBackend(t, foreign, exact)

	selection, err := backend.ResolveCandidatePoolSelection(
		context.Background(),
		candidatePodGang(),
		candidatePodClique(),
	)
	require.NoError(t, err)
	require.NotNil(t, selection)
	assert.Equal(t, types.UID("exact-uid"), selection.SourceUID)

	second := candidateLPXRequest("second", "second-uid", lpxv1alpha1.RequestPhaseBound)
	backend = candidateBackend(t, exact, second)
	_, err = backend.ResolveCandidatePoolSelection(
		context.Background(),
		candidatePodGang(),
		candidatePodClique(),
	)
	require.ErrorContains(t, err, "multiple live LPX requests match")

	backend = candidateBackend(t, foreign)
	selection, err = backend.ResolveCandidatePoolSelection(
		context.Background(),
		candidatePodGang(),
		candidatePodClique(),
	)
	require.NoError(t, err)
	assert.Nil(t, selection)
}

func TestResolveCandidatePoolSelectionRejectsNoncanonicalDomains(t *testing.T) {
	tests := []struct {
		name    string
		domains *[]string
	}{
		{name: "absent"},
		{name: "empty", domains: ptr.To([]string{})},
		{name: "empty value", domains: ptr.To([]string{""})},
		{name: "blank value", domains: ptr.To([]string{" "})},
		{name: "duplicate", domains: ptr.To([]string{"fabric-a", "fabric-a"})},
		{name: "unsorted", domains: ptr.To([]string{"fabric-b", "fabric-a"})},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := candidateLPXRequest(
				"request",
				"request-uid",
				lpxv1alpha1.RequestPhaseBound,
			)
			request.Status.ActivePlan.Placement.ActiveFabricIDs = test.domains
			backend := candidateBackend(t, request)

			_, err := backend.ResolveCandidatePoolSelection(
				context.Background(),
				candidatePodGang(),
				candidatePodClique(),
			)

			require.ErrorContains(t, err, "invalid active-plan fabric domains")
		})
	}
}

func TestResolveCandidatePoolSelectionRequiresCurrentCommittedPlan(t *testing.T) {
	t.Run("closed request remains pending", func(t *testing.T) {
		request := candidateLPXRequest(
			"request",
			"request-uid",
			lpxv1alpha1.RequestPhaseBound,
		)
		request.Spec.PlanningReady = false
		backend := candidateBackend(t, request)

		selection, err := backend.ResolveCandidatePoolSelection(
			context.Background(),
			candidatePodGang(),
			candidatePodClique(),
		)

		require.NoError(t, err)
		require.NotNil(t, selection)
		assert.False(t, selection.Eligible)
		assert.True(t, selection.SourcePlanObserved)
	})

	tests := []struct {
		name   string
		mutate func(*lpxv1alpha1.LPUPipelineRequest)
	}{
		{
			name: "stale observed generation",
			mutate: func(request *lpxv1alpha1.LPUPipelineRequest) {
				request.Status.ObservedGeneration = ptr.To(int64(4))
			},
		},
		{
			name: "stale planned generation",
			mutate: func(request *lpxv1alpha1.LPUPipelineRequest) {
				request.Status.ActivePlan.PlannedFromGeneration = 4
			},
		},
		{
			name: "unaccepted plan",
			mutate: func(request *lpxv1alpha1.LPUPipelineRequest) {
				request.Status.ActivePlan.AcceptedGeneration = nil
			},
		},
		{
			name: "stale accepted generation",
			mutate: func(request *lpxv1alpha1.LPUPipelineRequest) {
				request.Status.ActivePlan.AcceptedGeneration = ptr.To(int64(4))
			},
		},
		{
			name: "uncommitted fence",
			mutate: func(request *lpxv1alpha1.LPUPipelineRequest) {
				request.Status.CommitFence.Outcome = lpxv1alpha1.CommitFenceOutcomeAborted
			},
		},
		{
			name: "non-hybrid mode",
			mutate: func(request *lpxv1alpha1.LPUPipelineRequest) {
				request.Spec.WorkloadMode = lpxv1alpha1.WorkloadModeV3HxLPUOnly
				request.Status.ActivePlan.Placement.WorkloadMode =
					lpxv1alpha1.WorkloadModeV3HxLPUOnly
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := candidateLPXRequest(
				"request",
				"request-uid",
				lpxv1alpha1.RequestPhaseBound,
			)
			test.mutate(request)
			backend := candidateBackend(t, request)

			_, err := backend.ResolveCandidatePoolSelection(
				context.Background(),
				candidatePodGang(),
				candidatePodClique(),
			)

			require.ErrorContains(t, err, "no current committed plan")
		})
	}
}

func candidateBackend(
	t *testing.T,
	requests ...*lpxv1alpha1.LPUPipelineRequest,
) *schedulerBackend {
	t.Helper()
	objects := make([]client.Object, len(requests))
	for i := range requests {
		objects[i] = requests[i]
	}
	return New(
		testutils.CreateDefaultFakeClient(objects),
		configv1alpha1.SchedulerProfile{Name: configv1alpha1.SchedulerNameLPX},
	).(*schedulerBackend)
}

func candidateLPXRequest(
	name string,
	uid types.UID,
	phase lpxv1alpha1.RequestPhase,
) *lpxv1alpha1.LPUPipelineRequest {
	return &lpxv1alpha1.LPUPipelineRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Namespace:  "default",
			UID:        uid,
			Generation: 5,
		},
		Spec: lpxv1alpha1.LPUPipelineRequestSpec{
			PlanningReady: true,
			WorkloadMode:  lpxv1alpha1.WorkloadModeV3HxStrictHybrid,
			CyborgPodCliqueRef: &lpxv1alpha1.CyborgPodCliqueReference{
				Name: "model-0-candidate",
				UID:  "candidate-uid",
			},
			PodGangRef: lpxv1alpha1.NamespacedName{
				Name:      "model-0",
				Namespace: ptr.To("default"),
			},
		},
		Status: &lpxv1alpha1.LPUPipelineRequestStatus{
			Phase:              phase,
			ObservedGeneration: ptr.To(int64(5)),
			LastPlanRevision:   7,
			CommitFence: &lpxv1alpha1.CommitFence{
				ID:      "sha256:fence",
				Outcome: lpxv1alpha1.CommitFenceOutcomeCommitted,
			},
			ActivePlan: &lpxv1alpha1.ActivePlan{
				Revision:              7,
				PlannedFromGeneration: 5,
				AcceptedGeneration:    ptr.To(int64(5)),
				PlanDigest:            "plan-digest",
				Placement: lpxv1alpha1.PlanPlacement{
					ActiveFabricIDs: ptr.To([]string{"fabric-a", "fabric-c"}),
					WorkloadMode:    lpxv1alpha1.WorkloadModeV3HxStrictHybrid,
					PodGangRef: lpxv1alpha1.ObjectReference{
						Name:      "model-0",
						Namespace: "default",
						UID:       "pod-gang-uid",
					},
				},
			},
		},
	}
}

func candidatePodGang() *groveschedulerv1alpha1.PodGang {
	return &groveschedulerv1alpha1.PodGang{ObjectMeta: metav1.ObjectMeta{
		Name:      "model-0",
		Namespace: "default",
		UID:       "pod-gang-uid",
	}}
}

func candidatePodClique() *grovecorev1alpha1.PodClique {
	return &grovecorev1alpha1.PodClique{ObjectMeta: metav1.ObjectMeta{
		Name:      "model-0-candidate",
		Namespace: "default",
		UID:       "candidate-uid",
	}}
}
