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
	"errors"
	"fmt"
	"slices"
	"strings"

	configv1alpha1 "github.com/ai-dynamo/grove/operator/api/config/v1alpha1"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/ai-dynamo/grove/operator/internal/scheduler"

	groveschedulerv1alpha1 "github.com/ai-dynamo/grove/scheduler/api/core/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	allocationName       = "lpx-scheduler-active-plan"
	allocationAPIGroup   = "scheduling.lpu.nvidia.com"
	allocationAPIVersion = "v1alpha1"
	allocationKind       = "LpuPipelineAllocation"
)

var (
	errTopologyConstraintsUnsupported = errors.New(
		"lpx-scheduler does not support Grove topology constraints; " +
			"placement topology must be expressed through the LPX workload contract",
	)

	_ scheduler.Backend              = (*schedulerBackend)(nil)
	_ scheduler.CandidatePoolBackend = (*schedulerBackend)(nil)
)

type schedulerBackend struct {
	client client.Client
	name   string
}

// New creates an LPX scheduler backend.
func New(cl client.Client, profile configv1alpha1.SchedulerProfile) scheduler.Backend {
	return &schedulerBackend{
		client: cl,
		name:   string(profile.Name),
	}
}

func (b *schedulerBackend) Name() string {
	return b.name
}

func (b *schedulerBackend) Init() error {
	return nil
}

func (b *schedulerBackend) SyncPodGang(
	_ context.Context,
	_ *groveschedulerv1alpha1.PodGang,
) error {
	return nil
}

func (b *schedulerBackend) OnPodGangDelete(
	_ context.Context,
	_ *groveschedulerv1alpha1.PodGang,
) error {
	return nil
}

func (b *schedulerBackend) PreparePod(pod *corev1.Pod) {
	pod.Spec.SchedulerName = b.name
}

func (b *schedulerBackend) ValidatePodCliqueSet(
	_ context.Context,
	pcs *grovecorev1alpha1.PodCliqueSet,
) error {
	if pcs.Spec.Template.TopologyConstraint != nil {
		return errTopologyConstraintsUnsupported
	}
	for _, clique := range pcs.Spec.Template.Cliques {
		if clique != nil && clique.TopologyConstraint != nil {
			return errTopologyConstraintsUnsupported
		}
	}
	for _, scalingGroup := range pcs.Spec.Template.PodCliqueScalingGroupConfigs {
		if scalingGroup.TopologyConstraint != nil {
			return errTopologyConstraintsUnsupported
		}
	}
	return nil
}

// AllocationGVK returns the LPX allocation kind consumed by the candidate-pool backend.
func AllocationGVK() schema.GroupVersionKind {
	return schema.GroupVersionKind{
		Group:   allocationAPIGroup,
		Version: allocationAPIVersion,
		Kind:    allocationKind,
	}
}

// ResolveCandidatePoolSelection returns the exact committed LPX Pod cohort for podGang.
func (b *schedulerBackend) ResolveCandidatePoolSelection(
	ctx context.Context,
	podGang *groveschedulerv1alpha1.PodGang,
	pclq *grovecorev1alpha1.PodClique,
	pods []*corev1.Pod,
) (*scheduler.CandidatePoolSelection, error) {
	if b.client == nil {
		return nil, errors.New("LPX candidate-pool selection requires a Kubernetes client")
	}

	allocation := &unstructured.Unstructured{}
	allocation.SetGroupVersionKind(AllocationGVK())
	if err := b.client.Get(ctx, client.ObjectKey{Name: allocationName}, allocation); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("get LPX allocation %q: %w", allocationName, err)
	}

	matches, err := allocationMatchesPodGang(allocation.Object, podGang)
	if err != nil {
		return nil, err
	}
	if !matches {
		return nil, nil
	}

	phase, found, err := unstructured.NestedString(
		allocation.Object,
		"status",
		"phase",
	)
	if err != nil {
		return nil, fmt.Errorf("read LPX allocation status.phase: %w", err)
	}
	if !found || phase == "" {
		return nil, nil
	}
	if !selectionPhase(phase) {
		return nil, nil
	}

	planDigest, err := requiredNestedString(allocation.Object, "spec", "planDigest")
	if err != nil {
		return nil, err
	}
	placements, found, err := unstructured.NestedSlice(
		allocation.Object,
		"spec",
		"consumerPlacements",
	)
	if err != nil {
		return nil, fmt.Errorf("read LPX allocation consumer placements: %w", err)
	}
	if !found {
		return nil, errors.New("LPX allocation has no consumer placements")
	}

	candidatePods, err := candidatePodNames(podGang, pclq)
	if err != nil {
		return nil, err
	}
	podByUID := make(map[string]*corev1.Pod, len(pods))
	for _, pod := range pods {
		if pod != nil {
			podByUID[string(pod.UID)] = pod
		}
	}

	selected := make([]types.UID, 0)
	matchedCandidatePlacements := 0
	for i, rawPlacement := range placements {
		placement, ok := rawPlacement.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf(
				"LPX allocation consumer placement %d is not an object",
				i,
			)
		}
		podNamespace, err := requiredNestedString(
			placement,
			"podRef",
			"namespace",
		)
		if err != nil {
			return nil, fmt.Errorf(
				"read LPX allocation consumer placement %d Pod namespace: %w",
				i,
				err,
			)
		}
		podName, err := requiredNestedString(placement, "podRef", "name")
		if err != nil {
			return nil, fmt.Errorf(
				"read LPX allocation consumer placement %d Pod name: %w",
				i,
				err,
			)
		}
		if !candidatePods.Has(types.NamespacedName{
			Namespace: podNamespace,
			Name:      podName,
		}) {
			continue
		}
		matchedCandidatePlacements++

		podUID, err := requiredNestedString(placement, "podUid")
		if err != nil {
			return nil, fmt.Errorf(
				"read LPX allocation consumer placement %d Pod UID: %w",
				i,
				err,
			)
		}
		pod, ok := podByUID[podUID]
		if !ok {
			return nil, fmt.Errorf(
				"LPX allocation selected candidate Pod %s/%s UID %q, but that Pod is not current",
				podNamespace,
				podName,
				podUID,
			)
		}
		if podNamespace != pod.Namespace || podName != pod.Name {
			return nil, fmt.Errorf(
				"LPX allocation placement UID %q names %s/%s, but the current Pod is %s/%s",
				podUID,
				podNamespace,
				podName,
				pod.Namespace,
				pod.Name,
			)
		}
		selected = append(selected, pod.UID)
	}
	if matchedCandidatePlacements == 0 {
		return nil, fmt.Errorf(
			"LPX allocation %q has no selected candidate Pods for PodClique %s/%s",
			allocationName,
			pclq.Namespace,
			pclq.Name,
		)
	}

	slices.Sort(selected)
	selected = slices.Compact(selected)
	return &scheduler.CandidatePoolSelection{
		AllocationUID:        allocation.GetUID(),
		AllocationGeneration: allocation.GetGeneration(),
		PlanDigest:           planDigest,
		PodUIDs:              selected,
	}, nil
}

func allocationMatchesPodGang(
	allocation map[string]interface{},
	podGang *groveschedulerv1alpha1.PodGang,
) (bool, error) {
	name, err := requiredNestedString(allocation, "spec", "podGangRef", "name")
	if err != nil {
		return false, err
	}
	namespace, err := requiredNestedString(
		allocation,
		"spec",
		"podGangRef",
		"namespace",
	)
	if err != nil {
		return false, err
	}
	uid, err := requiredNestedString(allocation, "spec", "podGangRef", "uid")
	if err != nil {
		return false, err
	}
	return name == podGang.Name &&
		namespace == podGang.Namespace &&
		uid == string(podGang.UID), nil
}

func requiredNestedString(
	object map[string]interface{},
	fields ...string,
) (string, error) {
	fieldPath := strings.Join(fields, ".")
	value, found, err := unstructured.NestedString(object, fields...)
	if err != nil {
		return "", fmt.Errorf("read LPX allocation %s: %w", fieldPath, err)
	}
	if !found || value == "" {
		return "", fmt.Errorf("LPX allocation %s is required", fieldPath)
	}
	return value, nil
}

func candidatePodNames(
	podGang *groveschedulerv1alpha1.PodGang,
	pclq *grovecorev1alpha1.PodClique,
) (sets.Set[types.NamespacedName], error) {
	for _, group := range podGang.Spec.PodGroups {
		if group.Name != pclq.Name {
			continue
		}
		names := sets.New[types.NamespacedName]()
		for _, ref := range group.PodReferences {
			names.Insert(types.NamespacedName{
				Namespace: ref.Namespace,
				Name:      ref.Name,
			})
		}
		return names, nil
	}
	return nil, fmt.Errorf(
		"candidate-pool PodGang %s/%s has no PodGroup for PodClique %s/%s",
		podGang.Namespace,
		podGang.Name,
		pclq.Namespace,
		pclq.Name,
	)
}

func selectionPhase(phase string) bool {
	switch phase {
	case "bound", "degraded":
		return true
	default:
		return false
	}
}
