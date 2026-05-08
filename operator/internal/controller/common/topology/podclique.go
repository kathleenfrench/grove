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

package topology

import (
	"context"
	"fmt"

	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	componentutils "github.com/ai-dynamo/grove/operator/internal/controller/common/component/utils"
	"github.com/ai-dynamo/grove/operator/internal/controller/nodelabels"
	internalutils "github.com/ai-dynamo/grove/operator/internal/utils"

	"github.com/samber/lo"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ResolvePodCliqueTopologyAffinityStatus resolves topology status for a PodClique. It returns
// nil when the PodClique has no topologyAffinity.
func ResolvePodCliqueTopologyAffinityStatus(ctx context.Context, cl client.Client, nodeLabels nodelabels.Cache, pcs *grovecorev1alpha1.PodCliqueSet, pclq *grovecorev1alpha1.PodClique) (*grovecorev1alpha1.PodCliqueTopologyAffinityStatus, error) {
	if pclq.Spec.Affinity == nil || pclq.Spec.Affinity.TopologyAffinity == nil {
		return nil, nil
	}

	affinity := pclq.Spec.Affinity.TopologyAffinity
	labelKey, allDomains, err := ValuesForAffinity(ctx, cl, nodeLabels, affinity)
	if err != nil {
		return nil, err
	}

	associatedDomains, associatedReady, err := AssociatedScheduledDomains(ctx, cl, nodeLabels, pcs, pclq, affinity, labelKey)
	if err != nil {
		return nil, err
	}
	targetDomains := allDomains
	if associatedReady {
		targetDomains = associatedDomains
	}

	return &grovecorev1alpha1.PodCliqueTopologyAffinityStatus{
		LabelKey:          labelKey,
		AllDomains:        allDomains,
		AssociatedDomains: associatedDomains,
		TargetDomains:     targetDomains,
		AssociatedReady:   associatedReady,
	}, nil
}

// PodCliqueTemplateForPodClique returns the PodClique template for a concrete PodClique.
func PodCliqueTemplateForPodClique(pcs *grovecorev1alpha1.PodCliqueSet, pclq *grovecorev1alpha1.PodClique) (*grovecorev1alpha1.PodCliqueTemplateSpec, error) {
	cliqueName, err := internalutils.GetPodCliqueNameFromPodCliqueFQN(pclq.ObjectMeta)
	if err != nil {
		return nil, err
	}
	template := componentutils.FindPodCliqueTemplateSpecByName(pcs, cliqueName)
	if template == nil {
		return nil, fmt.Errorf("PodClique template %q not found in PodCliqueSet %q", cliqueName, pcs.Name)
	}
	return template, nil
}

// AssociatedScheduledDomains returns the union of topology domains used by the
// associated cliqueNames and whether all associated PodCliques have met their
// scheduled minimum.
func AssociatedScheduledDomains(ctx context.Context, cl client.Client, nodeLabels nodelabels.Cache, pcs *grovecorev1alpha1.PodCliqueSet, pclq *grovecorev1alpha1.PodClique, affinity *grovecorev1alpha1.TopologyAffinity, labelKey string) ([]string, bool, error) {
	associatedPCLQNames, err := AssociatedPodCliqueFQNs(pcs, pclq, affinity)
	if err != nil {
		return nil, false, err
	}

	associatedDomains := sets.New[string]()
	associatedReady := true
	for _, associatedPCLQName := range associatedPCLQNames {
		associatedPCLQ := &grovecorev1alpha1.PodClique{}
		if err = cl.Get(ctx, client.ObjectKey{Namespace: pclq.Namespace, Name: associatedPCLQName}, associatedPCLQ); err != nil {
			return nil, false, client.IgnoreNotFound(err)
		}

		minScheduled, ok := associatedPCLQ.MinAvailable()
		if !ok || associatedPCLQ.Status.ScheduledReplicas < minScheduled {
			associatedReady = false
		}

		pods, err := componentutils.GetPCLQPods(ctx, cl, pcs.Name, associatedPCLQ)
		if err != nil {
			return nil, false, err
		}
		for _, pod := range pods {
			if pod.Spec.NodeName == "" {
				continue
			}
			value, err := nodeLabels.ValueForNode(ctx, labelKey, pod.Spec.NodeName)
			if err != nil {
				return nil, false, fmt.Errorf("failed to resolve topology label %q for node %q: %w", labelKey, pod.Spec.NodeName, err)
			}
			associatedDomains.Insert(value)
		}
	}

	return sets.List(associatedDomains), associatedReady, nil
}

// AssociatedPodCliqueFQNs resolves unqualified cliqueNames in topologyAffinity
// into concrete PodClique names for this PodCliqueSet replica.
func AssociatedPodCliqueFQNs(pcs *grovecorev1alpha1.PodCliqueSet, pclq *grovecorev1alpha1.PodClique, affinity *grovecorev1alpha1.TopologyAffinity) ([]string, error) {
	pcsReplicaIndex, err := internalutils.GetPodCliqueSetReplicaIndexFromPodCliqueFQN(pcs.Name, pclq.Name)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(affinity.CliqueNames))
	for _, cliqueName := range affinity.CliqueNames {
		names = append(names, componentutils.GenerateDependencyNamesForBasePodGang(pcs, pcsReplicaIndex, cliqueName)...)
	}
	return lo.Uniq(names), nil
}
