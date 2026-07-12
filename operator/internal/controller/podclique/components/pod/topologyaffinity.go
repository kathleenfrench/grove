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
	"fmt"
	"maps"
	"slices"
	"sort"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	"github.com/ai-dynamo/grove/operator/internal/controller/common/component"
	groveerr "github.com/ai-dynamo/grove/operator/internal/errors"
	"github.com/ai-dynamo/grove/operator/internal/index"
	"github.com/ai-dynamo/grove/operator/internal/utils"
	k8sutils "github.com/ai-dynamo/grove/operator/internal/utils/kubernetes"

	"github.com/go-logr/logr"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (r _resource) syncTopologyAffinityPods(logger logr.Logger, sc *syncContext) error {
	if len(sc.topologyAffinity.TargetDomains) == 0 {
		logger.Info("No topology domains available for PodClique topology affinity", "pclq", client.ObjectKeyFromObject(sc.pclq))
	}

	podsToDelete := selectTopologyAffinityPodsToDelete(sc, logger)
	if len(podsToDelete) > 0 {
		if err := r.deleteSelectedPods(sc, logger, podsToDelete); err != nil {
			return err
		}
	}

	return r.createTopologyAffinityPods(sc.ctx, logger, sc)
}

func selectTopologyAffinityPodsToDelete(sc *syncContext, logger logr.Logger) []*corev1.Pod {
	targetDomains := sets.New(sc.topologyAffinity.TargetDomains...)
	podsByDomain := make(map[string][]*corev1.Pod, len(sc.topologyAffinity.TargetDomains))
	selected := make(map[string]*corev1.Pod)

	for _, pod := range sc.existingPCLQPods {
		if k8sutils.IsResourceTerminating(pod.ObjectMeta) {
			continue
		}
		value := pod.Labels[apicommon.LabelTopologyAffinityValue]
		if !targetDomains.Has(value) {
			selected[pod.Name] = pod
			continue
		}
		podsByDomain[value] = append(podsByDomain[value], pod)
	}

	expectedPerDomain := int(sc.pclq.Spec.Replicas)
	for domain := range targetDomains {
		pods := podsByDomain[domain]
		if len(pods) <= expectedPerDomain {
			continue
		}

		sorter := DeletionSorter{
			Pods:                    pods,
			ExpectedPodTemplateHash: sc.getExpectedPodTemplateHash(),
		}
		sort.Sort(sorter)

		for _, pod := range sorter.Pods[:len(pods)-expectedPerDomain] {
			selected[pod.Name] = pod
		}
	}

	if len(selected) > 0 {
		logger.Info("selected topology-affinity pods for deletion",
			"pclq", client.ObjectKeyFromObject(sc.pclq),
			"targetDomains", sc.topologyAffinity.TargetDomains,
			"pods", slices.Collect(maps.Keys(selected)),
		)
	}

	return slices.Collect(maps.Values(selected))
}

func (r _resource) deleteSelectedPods(sc *syncContext, logger logr.Logger, podsToDelete []*corev1.Pod) error {
	if len(podsToDelete) == 0 {
		return nil
	}
	deleteTasks := make([]utils.Task, 0, len(podsToDelete))
	for _, podToDelete := range podsToDelete {
		deleteTasks = append(deleteTasks, r.createPodDeletionTask(logger, sc.pclq, podToDelete, sc.pclqExpectationsStoreKey))
	}
	if runResult := utils.RunConcurrentlyWithSlowStart(sc.ctx, logger, 1, deleteTasks); runResult.HasErrors() {
		err := runResult.GetAggregatedError()
		pclqObjectKey := client.ObjectKeyFromObject(sc.pclq)
		logger.Error(err, "failed to delete topology-affinity pods for PCLQ", "runSummary", runResult.GetSummary())
		return groveerr.WrapError(err,
			errCodeDeletePod,
			component.OperationSync,
			fmt.Sprintf("failed to delete topology-affinity Pods for PodClique %v", pclqObjectKey),
		)
	}
	return nil
}

func (r _resource) createTopologyAffinityPods(ctx context.Context, logger logr.Logger, sc *syncContext) error {
	if len(sc.topologyAffinity.TargetDomains) == 0 {
		return groveerr.New(
			groveerr.ErrCodeRequeueAfter,
			component.OperationSync,
			"waiting for topology-affinity Node label values",
		)
	}
	topologyAffinity := sc.pclq.Spec.Affinity.TopologyAffinity
	createExpectations := r.expectationsStore.GetCreateExpectations(sc.pclqExpectationsStoreKey)
	if len(createExpectations) > 0 {
		logger.Info("waiting for topology-affinity create expectations to be observed",
			"pclq", client.ObjectKeyFromObject(sc.pclq),
			"numCreateExpectations", len(createExpectations),
		)
		return groveerr.New(
			groveerr.ErrCodeRequeueAfter,
			component.OperationSync,
			"waiting for topology-affinity Pod create expectations",
		)
	}

	deficits := topologyDomainDeficits(sc)
	numPods := lo.Reduce(lo.Values(deficits), func(agg int, deficit int, _ int) int {
		return agg + deficit
	}, 0)
	if numPods == 0 {
		return nil
	}

	availableIndices, err := index.GetAvailableIndices(logger, sc.existingPCLQPods, numPods)
	if err != nil {
		return groveerr.WrapError(err,
			errCodeGetAvailablePodHostNameIndices,
			component.OperationSync,
			fmt.Sprintf("error getting available indices for topology-affinity Pods in PodClique %v", client.ObjectKeyFromObject(sc.pclq)),
		)
	}

	createTasks := make([]utils.Task, 0, numPods)
	for _, value := range sc.topologyAffinity.TargetDomains {
		for range deficits[value] {
			index := availableIndices[0]
			availableIndices = availableIndices[1:]

			createTasks = append(
				createTasks, r.createPodCreationTask(
					logger,
					sc.pcs,
					sc.pclq,
					sc.associatedPodGangName,
					sc.pclqExpectationsStoreKey,
					index,
					index,
					addTopologyNodeAffinity(topologyAffinity.Domain, sc.topologyAffinity.LabelKey, value),
				),
			)
		}
	}

	runResult := utils.RunConcurrentlyWithSlowStart(ctx, logger, 1, createTasks)
	if runResult.HasErrors() {
		err = runResult.GetAggregatedError()
		logger.Error(err, "failed to create topology-affinity pods for PCLQ", "runSummary", runResult.GetSummary())
		return err
	}
	logger.Info("created topology-affinity pods", "numberOfCreatedPods", len(runResult.SuccessfulTasks))
	return groveerr.New(
		groveerr.ErrCodeRequeueAfter,
		component.OperationSync,
		"waiting for created topology-affinity Pods to be observed",
	)
}

func topologyDomainDeficits(sc *syncContext) map[string]int {
	expectedPerDomain := int(sc.pclq.Spec.Replicas)
	counts := make(map[string]int, len(sc.topologyAffinity.TargetDomains))
	targetDomains := sets.New(sc.topologyAffinity.TargetDomains...)
	for _, pod := range sc.existingPCLQPods {
		if k8sutils.IsResourceTerminating(pod.ObjectMeta) {
			continue
		}
		value := pod.Labels[apicommon.LabelTopologyAffinityValue]
		if targetDomains.Has(value) {
			counts[value]++
		}
	}

	deficits := make(map[string]int, len(sc.topologyAffinity.TargetDomains))
	for _, domain := range sc.topologyAffinity.TargetDomains {
		if counts[domain] < expectedPerDomain {
			deficits[domain] = expectedPerDomain - counts[domain]
		}
	}
	return deficits
}

func addTopologyNodeAffinity(domain, labelKey, value string) func(*corev1.Pod) {
	return func(pod *corev1.Pod) {
		if pod.Labels == nil {
			pod.Labels = make(map[string]string)
		}
		pod.Labels[apicommon.LabelTopologyAffinityDomain] = domain
		pod.Labels[apicommon.LabelTopologyAffinityValue] = value

		requirement := corev1.NodeSelectorRequirement{
			Key:      labelKey,
			Operator: corev1.NodeSelectorOpIn,
			Values:   []string{value},
		}

		if pod.Spec.Affinity == nil {
			pod.Spec.Affinity = &corev1.Affinity{}
		}

		if pod.Spec.Affinity.NodeAffinity == nil {
			pod.Spec.Affinity.NodeAffinity = &corev1.NodeAffinity{}
		}

		required := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
		if required == nil {
			pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{requirement}}},
			}
			return
		}

		if len(required.NodeSelectorTerms) == 0 {
			required.NodeSelectorTerms = []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{requirement}}}
			return
		}

		for i := range required.NodeSelectorTerms {
			required.NodeSelectorTerms[i].MatchExpressions = append(required.NodeSelectorTerms[i].MatchExpressions, requirement)
		}
	}
}
