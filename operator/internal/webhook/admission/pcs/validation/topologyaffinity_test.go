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

package validation

import (
	"testing"

	configv1alpha1 "github.com/ai-dynamo/grove/operator/api/config/v1alpha1"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const topologyAffinityTestKey = "topology.grove.io/block"

func TestValidatePodCliqueTopologyAffinity(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, grovecorev1alpha1.AddToScheme(scheme))

	tests := []struct {
		name        string
		operation   admissionv1.Operation
		subResource string
		mutateGPU   func(*grovecorev1alpha1.PodCliqueTemplateSpec)
		wantErrText string
	}{
		{
			name:      "valid topology affinity",
			operation: admissionv1.Create,
		},
		{
			name:      "minAvailable must equal replicas",
			operation: admissionv1.Create,
			mutateGPU: func(gpu *grovecorev1alpha1.PodCliqueTemplateSpec) {
				gpu.Spec.Replicas = 2
				gpu.Spec.MinAvailable = ptr.To[int32](1)
			},
			wantErrText: "minAvailable must equal replicas",
		},
		{
			name:        "scale subresource update allows replicas above minAvailable",
			operation:   admissionv1.Update,
			subResource: scaleSubResource,
			mutateGPU: func(gpu *grovecorev1alpha1.PodCliqueTemplateSpec) {
				gpu.Spec.Replicas = 2
				gpu.Spec.MinAvailable = ptr.To[int32](1)
			},
		},
		{
			name:        "scale subresource update rejects replicas below minAvailable",
			operation:   admissionv1.Update,
			subResource: scaleSubResource,
			mutateGPU: func(gpu *grovecorev1alpha1.PodCliqueTemplateSpec) {
				gpu.Spec.Replicas = 1
				gpu.Spec.MinAvailable = ptr.To[int32](2)
			},
			wantErrText: "minAvailable must equal replicas",
		},
		{
			name:      "nodeSelector cannot use topology key",
			operation: admissionv1.Create,
			mutateGPU: func(gpu *grovecorev1alpha1.PodCliqueTemplateSpec) {
				gpu.Spec.PodSpec.NodeSelector = map[string]string{topologyAffinityTestKey: "a"}
			},
			wantErrText: "nodeSelector conflicts with topologyAffinity",
		},
		{
			name:      "nodeAffinity cannot use topology key",
			operation: admissionv1.Create,
			mutateGPU: func(gpu *grovecorev1alpha1.PodCliqueTemplateSpec) {
				gpu.Spec.PodSpec.Affinity = &corev1.Affinity{
					NodeAffinity: &corev1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
							NodeSelectorTerms: []corev1.NodeSelectorTerm{{
								MatchExpressions: []corev1.NodeSelectorRequirement{{
									Key:      topologyAffinityTestKey,
									Operator: corev1.NodeSelectorOpIn,
									Values:   []string{"a"},
								}},
							}},
						},
					},
				}
			},
			wantErrText: "nodeAffinity conflicts with topologyAffinity",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pcs, gpu := topologyAffinityValidationPCS()
			if tc.mutateGPU != nil {
				tc.mutateGPU(gpu)
			}
			cl := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(topologyAffinityClusterTopology()).
				Build()
			validator := newPCSValidator(pcs, tc.operation, configv1alpha1.TopologyAwareSchedulingConfiguration{Enabled: true}, configv1alpha1.SchedulerConfiguration{}, cl, testutils.NewDefaultFakeRegistry(), tc.subResource)

			errs := validator.validatePodCliqueTopologyAffinity(gpu, field.NewPath("spec", "template", "cliques").Index(1))
			if tc.wantErrText == "" {
				assert.Empty(t, errs)
				return
			}
			require.NotEmpty(t, errs)
			assert.Contains(t, errs.ToAggregate().Error(), tc.wantErrText)
		})
	}
}

func TestValidateTopologyAffinityDependenciesRejectsCycles(t *testing.T) {
	aMin := int32(1)
	bMin := int32(1)
	cliques := []*grovecorev1alpha1.PodCliqueTemplateSpec{
		{
			Name: "a",
			Spec: grovecorev1alpha1.PodCliqueSpec{
				Replicas:     1,
				MinAvailable: &aMin,
				Affinity: &grovecorev1alpha1.PodCliqueAffinity{TopologyAffinity: &grovecorev1alpha1.TopologyAffinity{
					TopologyName: "fabric",
					Domain:       "block",
					CliqueNames:  []string{"b"},
				}},
			},
		},
		{
			Name: "b",
			Spec: grovecorev1alpha1.PodCliqueSpec{
				Replicas:     1,
				MinAvailable: &bMin,
				Affinity: &grovecorev1alpha1.PodCliqueAffinity{TopologyAffinity: &grovecorev1alpha1.TopologyAffinity{
					TopologyName: "fabric",
					Domain:       "block",
					CliqueNames:  []string{"a"},
				}},
			},
		},
	}

	errs := validateTopologyAffinityDependencies(cliques, field.NewPath("spec", "template", "cliques"))
	require.NotEmpty(t, errs)
	assert.Contains(t, errs.ToAggregate().Error(), "topologyAffinity must not have circular dependencies")
}

func topologyAffinityValidationPCS() (*grovecorev1alpha1.PodCliqueSet, *grovecorev1alpha1.PodCliqueTemplateSpec) {
	lpuMin := int32(2)
	gpuMin := int32(1)
	lpu := &grovecorev1alpha1.PodCliqueTemplateSpec{
		Name: "lpu",
		Spec: grovecorev1alpha1.PodCliqueSpec{
			Replicas:     2,
			MinAvailable: &lpuMin,
			PodSpec: corev1.PodSpec{Containers: []corev1.Container{{
				Name:  "lpu",
				Image: "example/lpu",
			}}},
		},
	}
	gpu := &grovecorev1alpha1.PodCliqueTemplateSpec{
		Name: "gpu",
		Spec: grovecorev1alpha1.PodCliqueSpec{
			Replicas:     1,
			MinAvailable: &gpuMin,
			PodSpec: corev1.PodSpec{Containers: []corev1.Container{{
				Name:  "gpu",
				Image: "example/gpu",
			}}},
			Affinity: &grovecorev1alpha1.PodCliqueAffinity{
				TopologyAffinity: &grovecorev1alpha1.TopologyAffinity{
					TopologyName: "fabric",
					Domain:       "block",
					CliqueNames:  []string{"lpu"},
				},
			},
		},
	}
	return &grovecorev1alpha1.PodCliqueSet{
		ObjectMeta: metav1.ObjectMeta{Name: "pcs", Namespace: "default"},
		Spec: grovecorev1alpha1.PodCliqueSetSpec{
			Replicas: 1,
			Template: grovecorev1alpha1.PodCliqueSetTemplateSpec{
				Cliques: []*grovecorev1alpha1.PodCliqueTemplateSpec{lpu, gpu},
			},
		},
	}, gpu
}

func topologyAffinityClusterTopology() *grovecorev1alpha1.ClusterTopologyBinding {
	return &grovecorev1alpha1.ClusterTopologyBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "fabric"},
		Spec: grovecorev1alpha1.ClusterTopologyBindingSpec{
			Levels: []grovecorev1alpha1.TopologyLevel{{
				Domain: grovecorev1alpha1.TopologyDomainBlock,
				Key:    topologyAffinityTestKey,
			}},
		},
	}
}
