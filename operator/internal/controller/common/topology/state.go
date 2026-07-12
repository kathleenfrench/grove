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
	"errors"
	"fmt"
	"slices"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/ai-dynamo/grove/operator/internal/controller/nodelabels"
	"github.com/ai-dynamo/grove/operator/internal/scheduler"

	groveschedulerv1alpha1 "github.com/ai-dynamo/grove/scheduler/api/core/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ResolvePodCliqueTopologyAffinityState resolves one reconciliation's topology
// state. External selections are observed only by status reconciliation so the
// selection is persisted before Pod reconciliation can prune any candidate.
func ResolvePodCliqueTopologyAffinityState(
	ctx context.Context,
	cl client.Client,
	nodeLabels nodelabels.Cache,
	schedRegistry scheduler.Registry,
	pcs *grovecorev1alpha1.PodCliqueSet,
	pclq *grovecorev1alpha1.PodClique,
	podGangName string,
	observeExternalSelection bool,
) (*grovecorev1alpha1.PodCliqueTopologyAffinityStatus, bool, error) {
	state, err := ResolvePodCliqueTopologyAffinityStatus(ctx, cl, nodeLabels, pcs, pclq)
	if err != nil {
		return nil, false, err
	}
	if state == nil {
		if pclq.Status.TopologyAffinity != nil &&
			pclq.Status.TopologyAffinity.Selection != nil {
			return nil, false, fmt.Errorf(
				"PodClique %s/%s removed topology affinity after committing a candidate selection",
				pclq.Namespace,
				pclq.Name,
			)
		}
		return nil, false, nil
	}
	backend, candidateBackend, finalizesCandidatePool, err :=
		candidatePoolBackendForPodCliqueSet(schedRegistry, pcs)
	if err != nil {
		return nil, false, err
	}
	if !finalizesCandidatePool {
		if pclq.Status.TopologyAffinity != nil &&
			pclq.Status.TopologyAffinity.Selection != nil {
			return nil, false, fmt.Errorf(
				"PodClique %s/%s has a committed candidate selection but its PodCliqueSet no longer uses a candidate-pool scheduler backend",
				pclq.Namespace,
				pclq.Name,
			)
		}
		return state, false, nil
	}
	if err = candidateBackend.ValidateCandidatePoolTopology(ctx, pclq, state.LabelKey); err != nil {
		return nil, true, err
	}

	existing := pclq.Status.TopologyAffinity
	preserveCandidatePoolState(existing, state, pclq.Generation)
	if existing != nil && existing.Selection != nil {
		if state.CandidatePoolObservedGeneration == nil ||
			*state.CandidatePoolObservedGeneration != pclq.Generation {
			return nil, true, fmt.Errorf(
				"committed candidate selection for PodClique %s/%s belongs to a different generation",
				pclq.Namespace,
				pclq.Name,
			)
		}
		if err = validateSelection(state.Selection, state.AllDomains); err != nil {
			return nil, true, fmt.Errorf("invalid persisted candidate selection: %w", err)
		}
		state.TargetDomains = slices.Clone(state.Selection.SelectedDomains)
		if !observeExternalSelection {
			return state, true, nil
		}

		observed, resolveErr := resolveCandidatePoolSelection(
			ctx,
			cl,
			backend,
			candidateBackend,
			pclq,
			podGangName,
		)
		if resolveErr != nil {
			return nil, true, resolveErr
		}
		if err = validatePersistedSelection(state.Selection, observed); err != nil {
			return nil, true, err
		}
		return state, true, nil
	}

	if state.CandidatePoolObservedGeneration != nil &&
		*state.CandidatePoolObservedGeneration == pclq.Generation {
		if err = validateCanonicalDomains(state.AllDomains); err != nil {
			return nil, true, fmt.Errorf("invalid frozen candidate-pool domains: %w", err)
		}
	}
	state.TargetDomains = slices.Clone(state.AllDomains)

	if !observeExternalSelection || state.CandidatePoolObservedGeneration == nil ||
		*state.CandidatePoolObservedGeneration != pclq.Generation {
		return state, true, nil
	}

	observed, err := resolveCandidatePoolSelection(
		ctx,
		cl,
		backend,
		candidateBackend,
		pclq,
		podGangName,
	)
	if err != nil {
		return nil, true, err
	}
	if observed == nil || !observed.Eligible {
		return state, true, nil
	}
	if err = validateObservedSelection(observed, state.AllDomains); err != nil {
		return nil, true, err
	}

	state.Selection = &grovecorev1alpha1.PodCliqueTopologyAffinitySelectionStatus{
		SourceUID:       string(observed.SourceUID),
		SourceRevision:  observed.SourceRevision,
		SourceDigest:    observed.SourceDigest,
		SelectedDomains: slices.Clone(observed.SelectedDomains),
	}
	state.TargetDomains = slices.Clone(observed.SelectedDomains)
	return state, true, nil
}

func candidatePoolBackendForPodCliqueSet(
	schedRegistry scheduler.Registry,
	pcs *grovecorev1alpha1.PodCliqueSet,
) (scheduler.Backend, scheduler.CandidatePoolBackend, bool, error) {
	var (
		selectedBackend   scheduler.Backend
		selectedCandidate scheduler.CandidatePoolBackend
	)
	for _, clique := range pcs.Spec.Template.Cliques {
		if clique == nil {
			continue
		}
		backend := schedRegistry.GetOrDefault(clique.Spec.PodSpec.SchedulerName)
		if backend == nil {
			return nil, nil, false, fmt.Errorf(
				"scheduler backend %q is not registered",
				clique.Spec.PodSpec.SchedulerName,
			)
		}
		candidate, ok := backend.(scheduler.CandidatePoolBackend)
		if !ok {
			continue
		}
		if selectedBackend != nil && selectedBackend.Name() != backend.Name() {
			return nil, nil, false, fmt.Errorf(
				"PodCliqueSet %s/%s uses multiple candidate-pool scheduler backends",
				pcs.Namespace,
				pcs.Name,
			)
		}
		selectedBackend = backend
		selectedCandidate = candidate
	}
	if selectedBackend == nil {
		return nil, nil, false, nil
	}
	return selectedBackend, selectedCandidate, true, nil
}

func resolveCandidatePoolSelection(
	ctx context.Context,
	cl client.Client,
	backend scheduler.Backend,
	candidateBackend scheduler.CandidatePoolBackend,
	pclq *grovecorev1alpha1.PodClique,
	podGangName string,
) (*scheduler.CandidatePoolObservation, error) {
	if podGangName == "" {
		return nil, fmt.Errorf(
			"topology-affinity PodClique %s/%s has no associated PodGang name",
			pclq.Namespace,
			pclq.Name,
		)
	}
	podGang := &groveschedulerv1alpha1.PodGang{}
	if err := cl.Get(
		ctx,
		client.ObjectKey{Namespace: pclq.Namespace, Name: podGangName},
		podGang,
	); err != nil {
		return nil, fmt.Errorf(
			"get candidate-pool PodGang %s/%s: %w",
			pclq.Namespace,
			podGangName,
			err,
		)
	}
	if !podGang.DeletionTimestamp.IsZero() {
		return nil, fmt.Errorf(
			"candidate-pool PodGang %s/%s UID %q is deleting",
			podGang.Namespace,
			podGang.Name,
			podGang.UID,
		)
	}
	if podGang.Labels[apicommon.LabelSchedulerName] != backend.Name() {
		return nil, fmt.Errorf(
			"candidate-pool PodGang %s/%s names scheduler backend %q, want %q",
			podGang.Namespace,
			podGang.Name,
			podGang.Labels[apicommon.LabelSchedulerName],
			backend.Name(),
		)
	}
	return candidateBackend.ResolveCandidatePoolSelection(ctx, podGang, pclq)
}

func preserveCandidatePoolState(
	existing *grovecorev1alpha1.PodCliqueTopologyAffinityStatus,
	state *grovecorev1alpha1.PodCliqueTopologyAffinityStatus,
	currentGeneration int64,
) {
	if existing == nil {
		return
	}
	if existing.CandidatePoolObservedGeneration != nil {
		generation := *existing.CandidatePoolObservedGeneration
		state.CandidatePoolObservedGeneration = &generation
		if generation == currentGeneration {
			state.AllDomains = slices.Clone(existing.AllDomains)
		}
	}
	if existing.Selection != nil {
		state.Selection = &grovecorev1alpha1.PodCliqueTopologyAffinitySelectionStatus{
			SourceUID:       existing.Selection.SourceUID,
			SourceRevision:  existing.Selection.SourceRevision,
			SourceDigest:    existing.Selection.SourceDigest,
			SelectedDomains: slices.Clone(existing.Selection.SelectedDomains),
		}
	}
}

func validateObservedSelection(
	selection *scheduler.CandidatePoolObservation,
	initialDomains []string,
) error {
	if !selection.Eligible || !selection.SourcePlanObserved {
		return errors.New("candidate selection source is not eligible to commit a selection")
	}
	if selection.SourceUID == "" {
		return errors.New("candidate selection source UID is empty")
	}
	if selection.SourceRevision <= 0 {
		return errors.New("candidate selection source revision must be positive")
	}
	if selection.SourceDigest == "" {
		return errors.New("candidate selection source digest is empty")
	}
	if err := validateSelectedDomains(selection.SelectedDomains, initialDomains); err != nil {
		return err
	}
	return nil
}

func validateSelection(
	selection *grovecorev1alpha1.PodCliqueTopologyAffinitySelectionStatus,
	initialDomains []string,
) error {
	return validateObservedSelection(&scheduler.CandidatePoolObservation{
		SourceUID:          types.UID(selection.SourceUID),
		SourceRevision:     selection.SourceRevision,
		SourceDigest:       selection.SourceDigest,
		SelectedDomains:    selection.SelectedDomains,
		SourcePlanObserved: true,
		Eligible:           true,
	}, initialDomains)
}

func validatePersistedSelection(
	persisted *grovecorev1alpha1.PodCliqueTopologyAffinitySelectionStatus,
	observed *scheduler.CandidatePoolObservation,
) error {
	if observed == nil {
		return nil
	}
	if persisted.SourceUID != string(observed.SourceUID) {
		return fmt.Errorf(
			"committed candidate selection changed source: persisted source %s, observed source %s",
			persisted.SourceUID,
			observed.SourceUID,
		)
	}
	if !observed.SourcePlanObserved {
		return nil
	}
	if persisted.SourceRevision != observed.SourceRevision ||
		persisted.SourceDigest != observed.SourceDigest ||
		!slices.Equal(persisted.SelectedDomains, observed.SelectedDomains) {
		return fmt.Errorf(
			"committed candidate selection changed: persisted source %s revision %d digest %q domains %v, observed source %s revision %d digest %q domains %v",
			persisted.SourceUID,
			persisted.SourceRevision,
			persisted.SourceDigest,
			persisted.SelectedDomains,
			observed.SourceUID,
			observed.SourceRevision,
			observed.SourceDigest,
			observed.SelectedDomains,
		)
	}
	return nil
}

func validateSelectedDomains(selectedDomains []string, initialDomains []string) error {
	if err := validateCanonicalDomains(initialDomains); err != nil {
		return fmt.Errorf("frozen initial domains are not canonical: %w", err)
	}
	if err := validateCanonicalDomains(selectedDomains); err != nil {
		return fmt.Errorf("selected domains are not canonical: %w", err)
	}
	initial := sets.New(initialDomains...)
	for _, domain := range selectedDomains {
		if !initial.Has(domain) {
			return fmt.Errorf(
				"selected domain %q is not in the frozen initial candidate pool",
				domain,
			)
		}
	}
	return nil
}

func validateCanonicalDomains(domains []string) error {
	if len(domains) == 0 {
		return errors.New("domains must be nonempty")
	}
	for i, domain := range domains {
		if domain == "" {
			return errors.New("domains must not contain an empty value")
		}
		if i > 0 && domains[i-1] >= domain {
			return errors.New("domains must be strictly sorted and unique")
		}
	}
	return nil
}
