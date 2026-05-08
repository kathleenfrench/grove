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
	"fmt"
	"os"
	"sort"
	"strings"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	apiconstants "github.com/ai-dynamo/grove/operator/api/common/constants"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	internalconstants "github.com/ai-dynamo/grove/operator/internal/constants"
	"github.com/ai-dynamo/grove/operator/internal/controller/common/component"
	commontopology "github.com/ai-dynamo/grove/operator/internal/controller/common/topology"
	groveerr "github.com/ai-dynamo/grove/operator/internal/errors"
	groveversion "github.com/ai-dynamo/grove/operator/internal/version"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
)

const (
	// envVarInitContainerImage stores the environment variable which is read to find the image for the init-container.
	// The environment variable should only store the registry and repository of the init-container. It should not contain any tag.
	envVarInitContainerImage string = "GROVE_INIT_CONTAINER_IMAGE"
	// initContainerName is the name of the init container.
	initContainerName = "grove-initc"
	// serviceAccountTokenSecretVolumeName is the name of the volume that mounts the service account token secret.
	serviceAccountTokenSecretVolumeName = "sa-token-secret-vol"
	// volumeMountPathServiceAccount is the base path where token and CA.cert for the service account will be placed.
	volumeMountPathServiceAccount = "/var/run/secrets/kubernetes.io/serviceaccount"
)

// configurePodInitContainer adds the necessary volumes and init container to the pod for dependency management
func configurePodInitContainer(pcs *grovecorev1alpha1.PodCliqueSet, pclq *grovecorev1alpha1.PodClique, pod *corev1.Pod) error {
	addServiceAccountTokenSecretVolume(pcs.Name, pod)
	return addInitContainer(pcs, pclq, pod)
}

// addServiceAccountTokenSecretVolume adds a volume that mounts the service account token secret
func addServiceAccountTokenSecretVolume(pcsName string, pod *corev1.Pod) {
	saTokenSecretVol := corev1.Volume{
		Name: serviceAccountTokenSecretVolumeName,
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName:  apicommon.GenerateInitContainerSATokenSecretName(pcsName),
				DefaultMode: ptr.To[int32](420),
			},
		},
	}
	pod.Spec.Volumes = append(pod.Spec.Volumes, saTokenSecretVol)
}

// addInitContainer adds the Grove init container to the pod with appropriate image, args, and volume mounts
func addInitContainer(pcs *grovecorev1alpha1.PodCliqueSet, pclq *grovecorev1alpha1.PodClique, pod *corev1.Pod) error {
	image, err := getInitContainerImage()
	if err != nil {
		return err
	}
	args, err := generateArgsForInitContainer(pcs, pclq)
	if err != nil {
		return err
	}

	pod.Spec.InitContainers = append(pod.Spec.InitContainers, corev1.Container{
		Name:  initContainerName,
		Image: fmt.Sprintf("%s:%s", image, groveversion.New().GitVersion),
		Args:  args,
		Env: []corev1.EnvVar{
			{
				Name: internalconstants.EnvVarPodNamespace,
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.namespace",
					},
				},
			},
		},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      serviceAccountTokenSecretVolumeName,
				ReadOnly:  true,
				MountPath: volumeMountPathServiceAccount,
			},
		},
	})
	return nil
}

// getInitContainerImage retrieves the init container image from environment variables
func getInitContainerImage() (string, error) {
	initContainerImage, ok := os.LookupEnv(envVarInitContainerImage)
	if !ok {
		return "", groveerr.New(
			errCodeInitContainerImageEnvVarMissing,
			component.OperationSync,
			fmt.Sprintf("environment variable %s specifying the init-container image is missing", envVarInitContainerImage),
		)
	}
	return initContainerImage, nil
}

// requiresPodInitContainer reports whether this PodClique's pods need the Grove
// init container to gate startup.
func requiresPodInitContainer(pclq *grovecorev1alpha1.PodClique) bool {
	return len(pclq.Spec.StartsAfter) > 0 || (pclq.Spec.Affinity != nil && pclq.Spec.Affinity.TopologyAffinity != nil)
}

// generateArgsForInitContainer creates command line arguments for the init container based on PodClique dependencies
func generateArgsForInitContainer(pcs *grovecorev1alpha1.PodCliqueSet, pclq *grovecorev1alpha1.PodClique) ([]string, error) {
	dependencies := make(map[string]struct{})
	conditions := make(map[string]string)
	for _, parentCliqueFQN := range pclq.Spec.StartsAfter {
		if err := addPodCliqueDependency(dependencies, pcs, parentCliqueFQN); err != nil {
			return nil, err
		}
	}

	if pclq.Spec.Affinity != nil && pclq.Spec.Affinity.TopologyAffinity != nil {
		affinity := pclq.Spec.Affinity.TopologyAffinity
		associatedPCLQNames, err := commontopology.AssociatedPodCliqueFQNs(pcs, pclq, affinity)
		if err != nil {
			return nil, err
		}
		conditions[pclq.Name] = apiconstants.ConditionTopologyAffinityReady
		for _, associatedPCLQName := range associatedPCLQNames {
			if err := addPodCliqueDependency(dependencies, pcs, associatedPCLQName); err != nil {
				return nil, err
			}
		}
	}

	args := make([]string, 0, len(dependencies)+len(conditions))
	seenArgs := make(map[string]struct{}, len(dependencies)+len(conditions))
	addArg := func(arg string) {
		if _, ok := seenArgs[arg]; ok {
			return
		}
		seenArgs[arg] = struct{}{}
		args = append(args, arg)
	}
	for name := range dependencies {
		addArg(fmt.Sprintf("--podcliques=%s", name))
	}
	for name, condition := range conditions {
		addArg(fmt.Sprintf("--podcliques=%s:%s", name, condition))
	}
	sort.Strings(args)
	return args, nil
}

func addPodCliqueDependency(dependencies map[string]struct{}, pcs *grovecorev1alpha1.PodCliqueSet, parentCliqueFQN string) error {
	_, ok := lo.Find(pcs.Spec.Template.Cliques, func(templateSpec *grovecorev1alpha1.PodCliqueTemplateSpec) bool {
		return strings.HasSuffix(parentCliqueFQN, templateSpec.Name)
	})
	if !ok {
		return groveerr.New(
			errCodeMissingPodCliqueTemplate,
			component.OperationSync,
			fmt.Sprintf("PodClique %s specified as an init-container dependency is not present in the templates", parentCliqueFQN),
		)
	}
	dependencies[parentCliqueFQN] = struct{}{}
	return nil
}
