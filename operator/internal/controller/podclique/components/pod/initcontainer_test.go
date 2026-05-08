// /*
// Copyright 2025 The Grove Authors.
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
	"testing"

	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	groveconstants "github.com/ai-dynamo/grove/operator/internal/constants"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestConfigurePodInitContainerUsesNamespaceEnvVar(t *testing.T) {
	t.Setenv(envVarInitContainerImage, "example.com/grove-initc")

	pcs := &grovecorev1alpha1.PodCliqueSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pcs",
		},
	}
	pclq := &grovecorev1alpha1.PodClique{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pclq",
			Namespace: "test-ns",
		},
	}
	pod := &corev1.Pod{}

	require.NoError(t, configurePodInitContainer(pcs, pclq, pod))
	require.Len(t, pod.Spec.InitContainers, 1)

	initContainer := pod.Spec.InitContainers[0]
	namespaceEnv := findEnvVar(initContainer.Env, groveconstants.EnvVarPodNamespace)
	require.NotNil(t, namespaceEnv)
	require.NotNil(t, namespaceEnv.ValueFrom)
	require.NotNil(t, namespaceEnv.ValueFrom.FieldRef)
	assert.Equal(t, "metadata.namespace", namespaceEnv.ValueFrom.FieldRef.FieldPath)

	for _, volume := range pod.Spec.Volumes {
		assert.Nil(t, volume.DownwardAPI)
	}
	require.Len(t, initContainer.VolumeMounts, 1)
	assert.Equal(t, serviceAccountTokenSecretVolumeName, initContainer.VolumeMounts[0].Name)
	assert.Equal(t, volumeMountPathServiceAccount, initContainer.VolumeMounts[0].MountPath)
}

func findEnvVar(envVars []corev1.EnvVar, name string) *corev1.EnvVar {
	for i := range envVars {
		if envVars[i].Name == name {
			return &envVars[i]
		}
	}
	return nil
}
