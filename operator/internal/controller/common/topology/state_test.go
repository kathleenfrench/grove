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

package topology

import (
	"context"
	"fmt"
	"testing"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	configv1alpha1 "github.com/ai-dynamo/grove/operator/api/config/v1alpha1"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/ai-dynamo/grove/operator/internal/scheduler"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"

	groveschedulerv1alpha1 "github.com/ai-dynamo/grove/scheduler/api/core/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const testFabricLabel = "network.topology.nvidia.com/lpu-fabric-pod"

type staticNodeLabels struct {
	values []string
}

func (c *staticNodeLabels) Values(context.Context, string) ([]string, error) {
	return c.values, nil
}

func (c *staticNodeLabels) ValueForNode(context.Context, string, string) (string, error) {
	return "", fmt.Errorf("not implemented")
}

type candidatePoolBackend struct {
	scheduler.Backend
	selection *scheduler.CandidatePoolObservation
	err       error
	calls     int
}

func (b *candidatePoolBackend) ValidateCandidatePoolTopology(
	_ context.Context,
	_ *grovecorev1alpha1.PodClique,
	resolvedLabelKey string,
) error {
	if resolvedLabelKey != testFabricLabel {
		return fmt.Errorf("unexpected topology label %q", resolvedLabelKey)
	}
	return nil
}

func (b *candidatePoolBackend) ResolveCandidatePoolSelection(
	context.Context,
	*groveschedulerv1alpha1.PodGang,
	*grovecorev1alpha1.PodClique,
) (*scheduler.CandidatePoolObservation, error) {
	b.calls++
	return b.selection, b.err
}

func TestResolveCandidateStateWaitsForPersistedCandidatePoolCompletion(t *testing.T) {
	pcs, pclq := candidateStateFixture()
	backend := newCandidatePoolBackend(candidateSelection("fabric-a"))

	state, candidateBackend, err := resolveCandidateState(
		pcs,
		pclq,
		backend,
		[]string{"fabric-a", "fabric-b"},
		true,
	)

	require.NoError(t, err)
	assert.True(t, candidateBackend)
	assert.Nil(t, state.Selection)
	assert.Equal(t, []string{"fabric-a", "fabric-b"}, state.TargetDomains)
	assert.Zero(t, backend.calls)
}

func TestResolveCandidateStateBootstrapsBeforePodGangExists(t *testing.T) {
	pcs, pclq := candidateStateFixture()
	backend := newCandidatePoolBackend(candidateSelection("fabric-a"))

	state, candidateBackend, err := ResolvePodCliqueTopologyAffinityState(
		context.Background(),
		testutils.CreateDefaultFakeClient([]client.Object{candidateTopologyBinding()}),
		&staticNodeLabels{values: []string{"fabric-a", "fabric-b"}},
		candidateRegistry(backend),
		pcs,
		pclq,
		"model-0",
		true,
	)

	require.NoError(t, err)
	assert.True(t, candidateBackend)
	assert.Equal(t, []string{"fabric-a", "fabric-b"}, state.TargetDomains)
	assert.Zero(t, backend.calls)
}

func TestResolveCandidateStateRejectsDeletingPodGang(t *testing.T) {
	pcs, pclq := candidateStateFixture()
	pclq.Status.TopologyAffinity = completedCandidatePool(pclq.Generation)
	backend := newCandidatePoolBackend(candidateSelection("fabric-a"))
	podGang := candidatePodGang()
	podGang.Finalizers = []string{"test/finalizer"}
	podGang.DeletionTimestamp = ptr.To(metav1.Now())

	_, _, err := ResolvePodCliqueTopologyAffinityState(
		context.Background(),
		testutils.CreateDefaultFakeClient([]client.Object{
			candidateTopologyBinding(),
			podGang,
		}),
		&staticNodeLabels{values: []string{"fabric-a", "fabric-b"}},
		candidateRegistry(backend),
		pcs,
		pclq,
		"model-0",
		true,
	)

	require.ErrorContains(t, err, "is deleting")
	assert.Zero(t, backend.calls)
}

func TestResolveCandidateStatePersistsSelectionBeforePruning(t *testing.T) {
	pcs, pclq := candidateStateFixture()
	pclq.Status.TopologyAffinity = completedCandidatePool(pclq.Generation)
	backend := newCandidatePoolBackend(candidateSelection("fabric-b"))

	specState, _, err := resolveCandidateState(
		pcs,
		pclq,
		backend,
		[]string{"fabric-a", "fabric-b"},
		false,
	)
	require.NoError(t, err)
	assert.Nil(t, specState.Selection)
	assert.Equal(t, []string{"fabric-a", "fabric-b"}, specState.TargetDomains)
	assert.Zero(t, backend.calls)

	statusState, _, err := resolveCandidateState(
		pcs,
		pclq,
		backend,
		[]string{"fabric-a", "fabric-b"},
		true,
	)
	require.NoError(t, err)
	require.NotNil(t, statusState.Selection)
	assert.Equal(t, []string{"fabric-b"}, statusState.Selection.SelectedDomains)
	assert.Equal(t, []string{"fabric-b"}, statusState.TargetDomains)
	assert.Equal(t, 1, backend.calls)
}

func TestResolveCandidateStateFreezesInitialDomains(t *testing.T) {
	pcs, pclq := candidateStateFixture()
	pclq.Status.TopologyAffinity = completedCandidatePool(pclq.Generation)
	backend := newCandidatePoolBackend(nil)

	state, _, err := resolveCandidateState(
		pcs,
		pclq,
		backend,
		[]string{"fabric-new"},
		false,
	)

	require.NoError(t, err)
	assert.Equal(t, []string{"fabric-a", "fabric-b"}, state.AllDomains)
	assert.Equal(t, []string{"fabric-a", "fabric-b"}, state.TargetDomains)
}

func TestResolveCandidateStateRejectsSelectionOutsideFrozenPool(t *testing.T) {
	pcs, pclq := candidateStateFixture()
	pclq.Status.TopologyAffinity = completedCandidatePool(pclq.Generation)
	backend := newCandidatePoolBackend(candidateSelection("fabric-c"))

	_, _, err := resolveCandidateState(
		pcs,
		pclq,
		backend,
		[]string{"fabric-a", "fabric-b"},
		true,
	)

	require.ErrorContains(t, err, "not in the frozen initial candidate pool")
}

func TestResolveCandidateStateRejectsCorruptFrozenPoolAfterSelection(t *testing.T) {
	pcs, pclq := candidateStateFixture()
	pclq.Status.TopologyAffinity = completedCandidatePool(pclq.Generation)
	pclq.Status.TopologyAffinity.AllDomains = []string{"fabric-b", "fabric-a"}
	pclq.Status.TopologyAffinity.Selection = persistedSelection("fabric-a")

	_, _, err := resolveCandidateState(
		pcs,
		pclq,
		newCandidatePoolBackend(nil),
		[]string{"fabric-a", "fabric-b"},
		false,
	)

	require.ErrorContains(t, err, "frozen initial domains are not canonical")
}

func TestResolveCandidateStateNeverRetargetsPersistedSelection(t *testing.T) {
	pcs, pclq := candidateStateFixture()
	pclq.Status.TopologyAffinity = completedCandidatePool(pclq.Generation)
	pclq.Status.TopologyAffinity.Selection = persistedSelection("fabric-a")
	pclq.Status.TopologyAffinity.TargetDomains = []string{"fabric-a"}

	t.Run("source disappearance preserves latch", func(t *testing.T) {
		backend := newCandidatePoolBackend(nil)
		state, _, err := resolveCandidateState(
			pcs,
			pclq,
			backend,
			[]string{"fabric-new"},
			true,
		)

		require.NoError(t, err)
		require.NotNil(t, state.Selection)
		assert.Equal(t, []string{"fabric-a"}, state.TargetDomains)
	})

	t.Run("same-plan release preserves latch", func(t *testing.T) {
		observation := candidateSelection("fabric-a")
		observation.Eligible = false
		backend := newCandidatePoolBackend(observation)
		state, _, err := resolveCandidateState(
			pcs,
			pclq,
			backend,
			[]string{"fabric-a", "fabric-b"},
			true,
		)

		require.NoError(t, err)
		require.NotNil(t, state.Selection)
		assert.Equal(t, []string{"fabric-a"}, state.TargetDomains)
	})

	t.Run("live replacement source fails before it has a plan", func(t *testing.T) {
		backend := newCandidatePoolBackend(&scheduler.CandidatePoolObservation{
			SourceUID: "replacement-request",
		})
		_, _, err := resolveCandidateState(
			pcs,
			pclq,
			backend,
			[]string{"fabric-a", "fabric-b"},
			true,
		)

		require.ErrorContains(t, err, "committed candidate selection changed source")
	})

	t.Run("changed plan fails before the source becomes eligible", func(t *testing.T) {
		observation := candidateSelection("fabric-a")
		observation.SourceDigest = "replacement-plan-digest"
		observation.Eligible = false
		backend := newCandidatePoolBackend(observation)
		_, _, err := resolveCandidateState(
			pcs,
			pclq,
			backend,
			[]string{"fabric-a", "fabric-b"},
			true,
		)

		require.ErrorContains(t, err, "committed candidate selection changed")
	})

	t.Run("changed domains are integrity error", func(t *testing.T) {
		backend := newCandidatePoolBackend(candidateSelection("fabric-b"))
		_, _, err := resolveCandidateState(
			pcs,
			pclq,
			backend,
			[]string{"fabric-a", "fabric-b"},
			true,
		)

		require.ErrorContains(t, err, "committed candidate selection changed")
	})

	t.Run("changed source identity is integrity error", func(t *testing.T) {
		selection := candidateSelection("fabric-a")
		selection.SourceUID = "replacement-request"
		backend := newCandidatePoolBackend(selection)
		_, _, err := resolveCandidateState(
			pcs,
			pclq,
			backend,
			[]string{"fabric-a", "fabric-b"},
			true,
		)

		require.ErrorContains(t, err, "committed candidate selection changed")
	})
}

func TestResolveCandidateStateRejectsTopologyAffinityRemovalAfterSelection(t *testing.T) {
	pcs, pclq := candidateStateFixture()
	pclq.Status.TopologyAffinity = completedCandidatePool(pclq.Generation)
	pclq.Status.TopologyAffinity.Selection = persistedSelection("fabric-a")
	pclq.Spec.Affinity = nil

	state, candidateBackend, err := ResolvePodCliqueTopologyAffinityState(
		context.Background(),
		testutils.CreateDefaultFakeClient(nil),
		&staticNodeLabels{},
		candidateRegistry(newCandidatePoolBackend(nil)),
		pcs,
		pclq,
		"model-0",
		false,
	)

	require.ErrorContains(t, err, "removed topology affinity after committing")
	assert.Nil(t, state)
	assert.False(t, candidateBackend)
}

func resolveCandidateState(
	pcs *grovecorev1alpha1.PodCliqueSet,
	pclq *grovecorev1alpha1.PodClique,
	backend *candidatePoolBackend,
	domains []string,
	observe bool,
) (*grovecorev1alpha1.PodCliqueTopologyAffinityStatus, bool, error) {
	objects := []client.Object{
		candidateTopologyBinding(),
		candidatePodGang(),
	}
	return ResolvePodCliqueTopologyAffinityState(
		context.Background(),
		testutils.CreateDefaultFakeClient(objects),
		&staticNodeLabels{values: domains},
		candidateRegistry(backend),
		pcs,
		pclq,
		"model-0",
		observe,
	)
}

func candidateRegistry(backend scheduler.Backend) scheduler.Registry {
	return &testutils.FakeSchedulerRegistry{
		Backends: map[string]scheduler.Backend{
			string(configv1alpha1.SchedulerNameLPX): backend,
			string(configv1alpha1.SchedulerNameKube): testutils.NewFakeSchedulerBackend(
				string(configv1alpha1.SchedulerNameKube),
			),
		},
		DefaultBackend: string(configv1alpha1.SchedulerNameKube),
	}
}

func candidateStateFixture() (
	*grovecorev1alpha1.PodCliqueSet,
	*grovecorev1alpha1.PodClique,
) {
	pcs := &grovecorev1alpha1.PodCliqueSet{
		ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "default"},
		Spec: grovecorev1alpha1.PodCliqueSetSpec{
			Template: grovecorev1alpha1.PodCliqueSetTemplateSpec{
				Cliques: []*grovecorev1alpha1.PodCliqueTemplateSpec{
					{
						Name: "conductor",
						Spec: grovecorev1alpha1.PodCliqueSpec{PodSpec: corev1.PodSpec{
							SchedulerName: string(configv1alpha1.SchedulerNameLPX),
						}},
					},
					{
						Name: "candidate",
						Spec: grovecorev1alpha1.PodCliqueSpec{PodSpec: corev1.PodSpec{
							SchedulerName: string(configv1alpha1.SchedulerNameKube),
						}},
					},
				},
			},
		},
	}
	pclq := &grovecorev1alpha1.PodClique{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "model-0-candidate",
			Namespace:  "default",
			UID:        "candidate-uid",
			Generation: 3,
		},
		Spec: grovecorev1alpha1.PodCliqueSpec{
			PodSpec: corev1.PodSpec{
				SchedulerName: string(configv1alpha1.SchedulerNameKube),
			},
			Replicas:     1,
			MinAvailable: ptr.To[int32](1),
			Affinity: &grovecorev1alpha1.PodCliqueAffinity{
				TopologyAffinity: &grovecorev1alpha1.TopologyAffinity{
					TopologyName: "fabric",
					Domain:       "fabric-pod",
				},
			},
		},
	}
	return pcs, pclq
}

func candidateTopologyBinding() *grovecorev1alpha1.ClusterTopologyBinding {
	return &grovecorev1alpha1.ClusterTopologyBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "fabric"},
		Spec: grovecorev1alpha1.ClusterTopologyBindingSpec{
			Levels: []grovecorev1alpha1.TopologyLevel{{
				Domain: "fabric-pod",
				Key:    testFabricLabel,
			}},
		},
	}
}

func candidatePodGang() *groveschedulerv1alpha1.PodGang {
	return &groveschedulerv1alpha1.PodGang{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "model-0",
			Namespace: "default",
			UID:       "pod-gang-uid",
			Labels: map[string]string{
				apicommon.LabelSchedulerName: string(configv1alpha1.SchedulerNameLPX),
			},
		},
	}
}

func newCandidatePoolBackend(
	selection *scheduler.CandidatePoolObservation,
) *candidatePoolBackend {
	return &candidatePoolBackend{
		Backend: testutils.NewFakeSchedulerBackend(
			string(configv1alpha1.SchedulerNameLPX),
		),
		selection: selection,
	}
}

func candidateSelection(domains ...string) *scheduler.CandidatePoolObservation {
	return &scheduler.CandidatePoolObservation{
		SourceUID:          types.UID("request-uid"),
		SourceRevision:     7,
		SourceDigest:       "plan-digest",
		SelectedDomains:    domains,
		SourcePlanObserved: true,
		Eligible:           true,
	}
}

func completedCandidatePool(
	generation int64,
) *grovecorev1alpha1.PodCliqueTopologyAffinityStatus {
	return &grovecorev1alpha1.PodCliqueTopologyAffinityStatus{
		LabelKey:                        testFabricLabel,
		AllDomains:                      []string{"fabric-a", "fabric-b"},
		TargetDomains:                   []string{"fabric-a", "fabric-b"},
		CandidatePoolObservedGeneration: ptr.To(generation),
	}
}

func persistedSelection(
	domains ...string,
) *grovecorev1alpha1.PodCliqueTopologyAffinitySelectionStatus {
	return &grovecorev1alpha1.PodCliqueTopologyAffinitySelectionStatus{
		SourceUID:       "request-uid",
		SourceRevision:  7,
		SourceDigest:    "plan-digest",
		SelectedDomains: domains,
	}
}
