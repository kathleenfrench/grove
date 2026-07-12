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
	"strings"

	configv1alpha1 "github.com/ai-dynamo/grove/operator/api/config/v1alpha1"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/ai-dynamo/grove/operator/internal/scheduler"

	groveschedulerv1alpha1 "github.com/ai-dynamo/grove/scheduler/api/core/v1alpha1"
	lpxv1alpha1 "github.com/nvidia-lpu/lpx-scheduler/api/go/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const lpuFabricTopologyLabel = "network.topology.nvidia.com/lpu-fabric-pod"

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

func (b *schedulerBackend) Init(_ client.Client) error {
	return nil
}

func (b *schedulerBackend) SyncPodGang(
	_ context.Context,
	_ *groveschedulerv1alpha1.PodGang,
) error {
	return nil
}

func (b *schedulerBackend) PreparePod(pod *corev1.Pod) error {
	pod.Spec.SchedulerName = b.name
	return nil
}

func (b *schedulerBackend) ValidatePodCliqueSet(
	ctx context.Context,
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

	return b.validateCandidatePoolTopologies(ctx, pcs)
}

func (b *schedulerBackend) ValidateCandidatePoolTopology(
	_ context.Context,
	pclq *grovecorev1alpha1.PodClique,
	resolvedLabelKey string,
) error {
	if resolvedLabelKey != lpuFabricTopologyLabel {
		return fmt.Errorf(
			"LPX candidate PodClique %s/%s resolves topology affinity to %q, want %q",
			pclq.Namespace,
			pclq.Name,
			resolvedLabelKey,
			lpuFabricTopologyLabel,
		)
	}
	return nil
}

// ResolveCandidatePoolSelection reads the immutable fabric-domain selection from
// the one live LPX request that exactly names the current PodGang and PodClique.
func (b *schedulerBackend) ResolveCandidatePoolSelection(
	ctx context.Context,
	podGang *groveschedulerv1alpha1.PodGang,
	pclq *grovecorev1alpha1.PodClique,
) (*scheduler.CandidatePoolObservation, error) {
	if b.client == nil {
		return nil, errors.New("LPX candidate-pool selection requires a Kubernetes client")
	}

	requests := &lpxv1alpha1.LPUPipelineRequestList{}
	if err := b.client.List(ctx, requests, client.InNamespace(podGang.Namespace)); err != nil {
		return nil, fmt.Errorf(
			"list LPX requests in namespace %q: %w",
			podGang.Namespace,
			err,
		)
	}

	var matching []*lpxv1alpha1.LPUPipelineRequest
	for i := range requests.Items {
		request := &requests.Items[i]
		if !request.DeletionTimestamp.IsZero() {
			continue
		}

		matches, err := requestMatchesCandidate(request, podGang, pclq)
		if err != nil {
			return nil, err
		}
		if matches {
			matching = append(matching, request)
		}
	}

	switch len(matching) {
	case 0:
		return nil, nil
	case 1:
	default:
		return nil, fmt.Errorf(
			"multiple live LPX requests match PodGang %s/%s UID %q and PodClique %s/%s UID %q",
			podGang.Namespace,
			podGang.Name,
			podGang.UID,
			pclq.Namespace,
			pclq.Name,
			pclq.UID,
		)
	}

	request := matching[0]
	if request.UID == "" {
		return nil, fmt.Errorf("matching LPX request %s/%s has no UID", request.Namespace, request.Name)
	}
	observation := &scheduler.CandidatePoolObservation{SourceUID: request.UID}
	if request.Status == nil || request.Status.ActivePlan == nil {
		return observation, nil
	}

	plan := request.Status.ActivePlan
	if plan.Revision <= 0 {
		return nil, fmt.Errorf(
			"matching LPX request %s/%s has invalid active-plan revision %d",
			request.Namespace,
			request.Name,
			plan.Revision,
		)
	}
	if plan.PlanDigest == "" {
		return nil, fmt.Errorf(
			"matching LPX request %s/%s has an empty active-plan digest",
			request.Namespace,
			request.Name,
		)
	}
	domains, err := canonicalActiveFabricIDs(plan.Placement.ActiveFabricIDs)
	if err != nil {
		return nil, fmt.Errorf(
			"matching LPX request %s/%s has invalid active-plan fabric domains: %w",
			request.Namespace,
			request.Name,
			err,
		)
	}
	observation.SourceRevision = plan.Revision
	observation.SourceDigest = string(plan.PlanDigest)
	observation.SelectedDomains = domains
	observation.SourcePlanObserved = true

	if !selectionPhase(request.Status.Phase) {
		return observation, nil
	}
	if !request.Spec.PlanningReady {
		return observation, nil
	}

	if err := validateCurrentCommittedPlan(request, plan); err != nil {
		return nil, fmt.Errorf(
			"matching LPX request %s/%s has no current committed plan: %w",
			request.Namespace,
			request.Name,
			err,
		)
	}
	observation.Eligible = true
	return observation, nil
}

func validateCurrentCommittedPlan(
	request *lpxv1alpha1.LPUPipelineRequest,
	plan *lpxv1alpha1.ActivePlan,
) error {
	if request.Generation <= 0 {
		return errors.New("request generation must be positive")
	}
	if request.Status.ObservedGeneration == nil ||
		*request.Status.ObservedGeneration != request.Generation {
		return errors.New("status observedGeneration does not equal the request generation")
	}
	if plan.PlannedFromGeneration != request.Generation {
		return errors.New("active-plan plannedFromGeneration does not equal the request generation")
	}
	if plan.AcceptedGeneration == nil || *plan.AcceptedGeneration != request.Generation {
		return errors.New("active-plan acceptedGeneration does not equal the request generation")
	}
	if request.Status.LastPlanRevision != plan.Revision {
		return errors.New("status lastPlanRevision does not equal the active-plan revision")
	}
	if request.Status.CommitFence == nil ||
		request.Status.CommitFence.Outcome != lpxv1alpha1.CommitFenceOutcomeCommitted {
		return errors.New("status commitFence is not committed")
	}
	if !strictHybridMode(request.Spec.WorkloadMode) ||
		plan.Placement.WorkloadMode != request.Spec.WorkloadMode {
		return errors.New("active-plan workload mode is not the request's strict-hybrid mode")
	}
	return nil
}

func strictHybridMode(mode lpxv1alpha1.WorkloadMode) bool {
	switch mode {
	case lpxv1alpha1.WorkloadModeV2StrictHybrid,
		lpxv1alpha1.WorkloadModeV3HxStrictHybrid:
		return true
	default:
		return false
	}
}

func (b *schedulerBackend) validateCandidatePoolTopologies(
	ctx context.Context,
	pcs *grovecorev1alpha1.PodCliqueSet,
) error {
	for _, clique := range pcs.Spec.Template.Cliques {
		if clique == nil || clique.Spec.Affinity == nil ||
			clique.Spec.Affinity.TopologyAffinity == nil {
			continue
		}
		if b.client == nil {
			return errors.New("LPX topology-affinity validation requires a Kubernetes client")
		}

		affinity := clique.Spec.Affinity.TopologyAffinity
		binding := &grovecorev1alpha1.ClusterTopologyBinding{}
		if err := b.client.Get(ctx, client.ObjectKey{Name: affinity.TopologyName}, binding); err != nil {
			return fmt.Errorf(
				"get ClusterTopologyBinding %q for LPX candidate PodClique %q: %w",
				affinity.TopologyName,
				clique.Name,
				err,
			)
		}

		labelKey := ""
		for _, level := range binding.Spec.Levels {
			if string(level.Domain) == affinity.Domain {
				labelKey = level.Key
				break
			}
		}
		if labelKey == "" {
			return fmt.Errorf(
				"ClusterTopologyBinding %q has no domain %q for LPX candidate PodClique %q",
				affinity.TopologyName,
				affinity.Domain,
				clique.Name,
			)
		}
		if labelKey != lpuFabricTopologyLabel {
			return fmt.Errorf(
				"LPX candidate PodClique %q resolves topology affinity to %q, want %q",
				clique.Name,
				labelKey,
				lpuFabricTopologyLabel,
			)
		}
	}
	return nil
}

func requestMatchesCandidate(
	request *lpxv1alpha1.LPUPipelineRequest,
	podGang *groveschedulerv1alpha1.PodGang,
	pclq *grovecorev1alpha1.PodClique,
) (bool, error) {
	cyborgRef := request.Spec.CyborgPodCliqueRef
	cyborgRelated := cyborgRef != nil &&
		(cyborgRef.Name == pclq.Name || cyborgRef.UID == string(pclq.UID))

	specPodGangNamespace := ptr.Deref(request.Spec.PodGangRef.Namespace, "")
	specPodGangRelated := request.Spec.PodGangRef.Name == podGang.Name

	var activePodGangRelated bool
	if request.Status != nil && request.Status.ActivePlan != nil {
		activeRef := request.Status.ActivePlan.Placement.PodGangRef
		activePodGangRelated = activeRef.Name == podGang.Name ||
			activeRef.UID == string(podGang.UID)
	}

	if !cyborgRelated && !specPodGangRelated && !activePodGangRelated {
		return false, nil
	}
	if cyborgRef == nil || cyborgRef.Name != pclq.Name || cyborgRef.UID != string(pclq.UID) {
		return false, fmt.Errorf(
			"LPX request %s/%s has a contradictory candidate PodClique reference",
			request.Namespace,
			request.Name,
		)
	}
	if request.Spec.PodGangRef.Name != podGang.Name || specPodGangNamespace != podGang.Namespace {
		return false, fmt.Errorf(
			"LPX request %s/%s has a contradictory spec PodGang reference",
			request.Namespace,
			request.Name,
		)
	}
	if request.Status == nil || request.Status.ActivePlan == nil {
		return true, nil
	}

	activeRef := request.Status.ActivePlan.Placement.PodGangRef
	if activeRef.Name != podGang.Name || activeRef.Namespace != podGang.Namespace ||
		activeRef.UID != string(podGang.UID) {
		return false, fmt.Errorf(
			"LPX request %s/%s has a contradictory active-plan PodGang reference",
			request.Namespace,
			request.Name,
		)
	}
	return true, nil
}

func canonicalActiveFabricIDs(activeFabricIDs *[]string) ([]string, error) {
	if activeFabricIDs == nil || len(*activeFabricIDs) == 0 {
		return nil, errors.New("activeFabricIds must be nonempty")
	}
	if len(*activeFabricIDs) > 256 {
		return nil, errors.New("activeFabricIds exceeds the canonical maximum of 256 domains")
	}

	domains := append([]string(nil), (*activeFabricIDs)...)
	for i, domain := range domains {
		if strings.TrimSpace(domain) == "" {
			return nil, errors.New("activeFabricIds must not contain an empty domain")
		}
		if i > 0 && domains[i-1] >= domain {
			return nil, errors.New("activeFabricIds must be strictly sorted and unique")
		}
	}
	return domains, nil
}

func selectionPhase(phase lpxv1alpha1.RequestPhase) bool {
	switch phase {
	case lpxv1alpha1.RequestPhaseBound, lpxv1alpha1.RequestPhaseDegraded:
		return true
	default:
		return false
	}
}
