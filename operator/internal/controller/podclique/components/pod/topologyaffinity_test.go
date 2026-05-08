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

package pod

import (
	"context"
	"testing"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	apiconstants "github.com/ai-dynamo/grove/operator/api/common/constants"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/ai-dynamo/grove/operator/internal/expect"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
)

func TestAddTopologyNodeAffinity(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Affinity: &corev1.Affinity{
				NodeAffinity: &corev1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{{
							MatchExpressions: []corev1.NodeSelectorRequirement{{
								Key:      "accelerator",
								Operator: corev1.NodeSelectorOpExists,
							}},
						}},
					},
				},
			},
		},
	}

	addTopologyNodeAffinity("domain", "topology.grove.io/block", "fabric-a")(pod)

	terms := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	assert.Len(t, terms, 1)
	assert.Equal(t, []corev1.NodeSelectorRequirement{
		{Key: "accelerator", Operator: corev1.NodeSelectorOpExists},
		{Key: "topology.grove.io/block", Operator: corev1.NodeSelectorOpIn, Values: []string{"fabric-a"}},
	}, terms[0].MatchExpressions)
}

func TestGenerateArgsForInitContainerIncludesTopologyAffinityGate(t *testing.T) {
	pcs, pclq := topologyAffinityInitContainerFixture()

	assert.True(t, requiresPodInitContainer(pclq))

	args, err := generateArgsForInitContainer(pcs, pclq)
	require.NoError(t, err)
	assert.Equal(t, []string{
		"--podcliques=test-pcs-0-cyborg:" + apiconstants.ConditionTopologyAffinityReady,
		"--podcliques=test-pcs-0-lpu",
	}, args)
}

func TestGenerateArgsForInitContainerPreservesReadinessAndConditionForSamePodClique(t *testing.T) {
	pcs, pclq := topologyAffinityInitContainerFixture()
	pclq.Spec.StartsAfter = []string{"test-pcs-0-cyborg"}

	args, err := generateArgsForInitContainer(pcs, pclq)
	require.NoError(t, err)
	assert.Equal(t, []string{
		"--podcliques=test-pcs-0-cyborg",
		"--podcliques=test-pcs-0-cyborg:" + apiconstants.ConditionTopologyAffinityReady,
		"--podcliques=test-pcs-0-lpu",
	}, args)
}

func TestCreateTopologyAffinityPodsWaitsForCreateExpectations(t *testing.T) {
	pcs, pclq := topologyAffinityInitContainerFixture()
	expectationsStore := expect.NewExpectationsStore()
	const expectationsKey = "default/test-pcs-0-cyborg"
	require.NoError(t, expectationsStore.ExpectCreations(logr.Discard(), expectationsKey, types.UID("pending-create")))

	r := _resource{expectationsStore: expectationsStore}
	sc := &syncContext{
		ctx:                      context.Background(),
		pcs:                      pcs,
		pclq:                     pclq,
		pclqExpectationsStoreKey: expectationsKey,
		topologyAffinity: &grovecorev1alpha1.PodCliqueTopologyAffinityStatus{
			LabelKey:      "topology.grove.io/fabric-pod",
			TargetDomains: []string{"fabric-a"},
		},
	}

	require.NoError(t, r.createTopologyAffinityPods(context.Background(), logr.Discard(), sc))
}

func topologyAffinityInitContainerFixture() (*grovecorev1alpha1.PodCliqueSet, *grovecorev1alpha1.PodClique) {
	pcs := &grovecorev1alpha1.PodCliqueSet{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pcs", Namespace: "default"},
		Spec: grovecorev1alpha1.PodCliqueSetSpec{
			Template: grovecorev1alpha1.PodCliqueSetTemplateSpec{
				Cliques: []*grovecorev1alpha1.PodCliqueTemplateSpec{
					{
						Name: "lpu",
						Spec: grovecorev1alpha1.PodCliqueSpec{
							Replicas:     2,
							MinAvailable: ptr.To[int32](2),
						},
					},
					{
						Name: "cyborg",
						Spec: grovecorev1alpha1.PodCliqueSpec{
							Replicas:     2,
							MinAvailable: ptr.To[int32](2),
							Affinity: &grovecorev1alpha1.PodCliqueAffinity{
								TopologyAffinity: &grovecorev1alpha1.TopologyAffinity{
									TopologyName: "fabric",
									Domain:       "fabric-pod",
									CliqueNames:  []string{"lpu"},
								},
							},
						},
					},
				},
			},
		},
	}
	pclq := &grovecorev1alpha1.PodClique{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pcs-0-cyborg",
			Namespace: "default",
			Labels: map[string]string{
				apicommon.LabelPartOfKey:                "test-pcs",
				apicommon.LabelPodCliqueSetReplicaIndex: "0",
			},
		},
		Spec: *pcs.Spec.Template.Cliques[1].Spec.DeepCopy(),
	}
	return pcs, pclq
}
