// /*
// Copyright 2025 The Grove Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License")
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

package internal

import (
	"testing"

	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/ptr"
)

func TestNotifyIfAllParentsReady(t *testing.T) {
	tests := []struct {
		name              string
		dependencies      sets.Set[string]
		currentReadyPCLQs sets.Set[string]
		expected          bool
	}{
		{
			name:              "all_dependencies_met",
			dependencies:      sets.New("podclique-a", "podclique-b"),
			currentReadyPCLQs: sets.New("podclique-a", "podclique-b"),
			expected:          true,
		},
		{
			name:              "one_dependency_not_met",
			dependencies:      sets.New("podclique-a", "podclique-b"),
			currentReadyPCLQs: sets.New("podclique-a"),
			expected:          false,
		},
		{
			name:              "no_dependencies",
			dependencies:      sets.New[string](),
			currentReadyPCLQs: sets.New[string](),
			expected:          true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deps := &ParentPodCliqueDependencies{
				pclqFQNs:          tt.dependencies,
				currentReadyPCLQs: tt.currentReadyPCLQs,
				ready:             make(chan struct{}, 1),
			}

			deps.notifyIfAllParentsReady()

			result := false
			select {
			case <-deps.ready:
				result = true
			default:
			}

			assert.Equal(t, tt.expected, result, "Readiness check should match expected result")
		})
	}
}

func TestNewPodCliqueStateUsesProvidedNamespace(t *testing.T) {
	deps := NewPodCliqueState(sets.New("podclique-a"), map[string][]string{
		"podclique-b": {"TopologyAffinityReady"},
	}, "test-ns")

	assert.Equal(t, "test-ns", deps.namespace)
	assert.True(t, deps.pclqFQNs.Has("podclique-a"))
	assert.True(t, deps.pclqFQNToConditions["podclique-b"].Has("TopologyAffinityReady"))
}

func TestRefreshReadinessOfPodClique(t *testing.T) {
	tests := []struct {
		name                 string
		initialReadyPCLQs    sets.Set[string]
		pclq                 *grovecorev1alpha1.PodClique
		expectedReadyPCLQs   sets.Set[string]
		trackedPodCliqueFQNs sets.Set[string]
	}{
		{
			name:                 "ready_replicas_satisfy_min_available",
			initialReadyPCLQs:    sets.New[string](),
			pclq:                 podClique("podclique-a", 3, ptr.To[int32](2), 2, nil),
			expectedReadyPCLQs:   sets.New("podclique-a"),
			trackedPodCliqueFQNs: sets.New("podclique-a"),
		},
		{
			name:                 "ready_replicas_below_min_available",
			initialReadyPCLQs:    sets.New("podclique-a"),
			pclq:                 podClique("podclique-a", 3, ptr.To[int32](2), 1, nil),
			expectedReadyPCLQs:   sets.New[string](),
			trackedPodCliqueFQNs: sets.New("podclique-a"),
		},
		{
			name:                 "replicas_used_when_min_available_missing",
			initialReadyPCLQs:    sets.New[string](),
			pclq:                 podClique("podclique-a", 2, nil, 2, nil),
			expectedReadyPCLQs:   sets.New("podclique-a"),
			trackedPodCliqueFQNs: sets.New("podclique-a"),
		},
		{
			name:                 "topology_affinity_requires_expanded_ready_replicas",
			initialReadyPCLQs:    sets.New("podclique-a"),
			pclq:                 topologyAffinityPodClique("podclique-a", 2, 5, []string{"rack-a", "rack-b", "rack-c"}),
			expectedReadyPCLQs:   sets.New[string](),
			trackedPodCliqueFQNs: sets.New("podclique-a"),
		},
		{
			name:                 "topology_affinity_ready_when_expanded_replicas_satisfied",
			initialReadyPCLQs:    sets.New[string](),
			pclq:                 topologyAffinityPodClique("podclique-a", 2, 6, []string{"rack-a", "rack-b", "rack-c"}),
			expectedReadyPCLQs:   sets.New("podclique-a"),
			trackedPodCliqueFQNs: sets.New("podclique-a"),
		},
		{
			name:                 "topology_affinity_missing_status_is_not_ready",
			initialReadyPCLQs:    sets.New("podclique-a"),
			pclq:                 topologyAffinityPodClique("podclique-a", 2, 2, nil),
			expectedReadyPCLQs:   sets.New[string](),
			trackedPodCliqueFQNs: sets.New("podclique-a"),
		},
		{
			name:                 "untracked_podclique_does_not_affect_state",
			initialReadyPCLQs:    sets.New[string](),
			pclq:                 podClique("podclique-b", 1, ptr.To[int32](1), 1, nil),
			expectedReadyPCLQs:   sets.New[string](),
			trackedPodCliqueFQNs: sets.New("podclique-a"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deps := &ParentPodCliqueDependencies{
				pclqFQNs:          tt.trackedPodCliqueFQNs,
				currentReadyPCLQs: tt.initialReadyPCLQs,
			}

			deps.refreshReadinessOfPodClique(tt.pclq)

			assert.Equal(t, tt.expectedReadyPCLQs, deps.currentReadyPCLQs, "Ready PodClique state should match expected")
		})
	}
}

func TestRefreshConditionsOfPodClique(t *testing.T) {
	deps := &ParentPodCliqueDependencies{
		pclqFQNToConditions: map[string]sets.Set[string]{
			"podclique-a": sets.New("TopologyAffinityReady"),
		},
		currentPCLQConditions: map[string]sets.Set[string]{
			"podclique-a": sets.New[string](),
		},
	}
	pclq := podClique("podclique-a", 1, ptr.To[int32](1), 1, map[string]string{
		"TopologyAffinityReady": string(metav1.ConditionTrue),
	})

	deps.refreshConditionsOfPodClique(pclq)
	assert.True(t, deps.currentPCLQConditions["podclique-a"].Has("TopologyAffinityReady"))
}

func topologyAffinityPodClique(name string, replicas, readyReplicas int32, targetDomains []string) *grovecorev1alpha1.PodClique {
	pclq := podClique(name, replicas, ptr.To(replicas), readyReplicas, nil)
	pclq.Spec.Affinity = &grovecorev1alpha1.PodCliqueAffinity{
		TopologyAffinity: &grovecorev1alpha1.TopologyAffinity{
			TopologyName: "fabric",
			Domain:       "rack",
			CliqueNames:  []string{"parent"},
		},
	}
	if targetDomains != nil {
		pclq.Status.TopologyAffinity = &grovecorev1alpha1.PodCliqueTopologyAffinityStatus{
			TargetDomains: targetDomains,
		}
	}
	return pclq
}

func TestNotifyIfAllParentsReadyRequiresConditions(t *testing.T) {
	deps := &ParentPodCliqueDependencies{
		pclqFQNs: sets.New("parent-pclq"),
		pclqFQNToConditions: map[string]sets.Set[string]{
			"child-pclq": sets.New("TopologyAffinityReady"),
		},
		currentReadyPCLQs: sets.New("parent-pclq"),
		currentPCLQConditions: map[string]sets.Set[string]{
			"child-pclq": sets.New[string](),
		},
		ready: make(chan struct{}, 1),
	}

	deps.notifyIfAllParentsReady()
	select {
	case <-deps.ready:
		t.Fatal("dependencies should not be ready until required conditions are true")
	default:
	}

	deps.currentPCLQConditions["child-pclq"].Insert("TopologyAffinityReady")
	deps.notifyIfAllParentsReady()
	select {
	case <-deps.ready:
	default:
		t.Fatal("dependencies should be ready")
	}
}

func podClique(name string, replicas int32, minAvailable *int32, readyReplicas int32, conditions map[string]string) *grovecorev1alpha1.PodClique {
	statusConditions := make([]metav1.Condition, 0, len(conditions))
	for conditionType, conditionStatus := range conditions {
		statusConditions = append(statusConditions, metav1.Condition{
			Type:   conditionType,
			Status: metav1.ConditionStatus(conditionStatus),
		})
	}

	return &grovecorev1alpha1.PodClique{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: grovecorev1alpha1.PodCliqueSpec{
			Replicas:     replicas,
			MinAvailable: minAvailable,
		},
		Status: grovecorev1alpha1.PodCliqueStatus{
			ReadyReplicas: readyReplicas,
			Conditions:    statusConditions,
		},
	}
}
