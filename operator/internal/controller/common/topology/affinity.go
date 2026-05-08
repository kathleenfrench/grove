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
	"github.com/ai-dynamo/grove/operator/internal/clustertopology"
	"github.com/ai-dynamo/grove/operator/internal/controller/nodelabels"

	"github.com/samber/lo"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// KeyForDomain resolves a ClusterTopology domain to its node label key.
func KeyForDomain(ctx context.Context, cl client.Client, topologyName string, domain grovecorev1alpha1.TopologyDomain) (string, error) {
	levels, err := clustertopology.GetClusterTopologyLevels(ctx, cl, topologyName)
	if err != nil {
		return "", err
	}
	level, ok := lo.Find(levels, func(level grovecorev1alpha1.TopologyLevel) bool {
		return level.Domain == domain
	})
	if !ok {
		return "", fmt.Errorf("topology domain %q not found in ClusterTopology %q", domain, topologyName)
	}
	return level.Key, nil
}

// ValuesForAffinity resolves the topology label key and currently known values
// for a PodClique topology affinity rule.
func ValuesForAffinity(ctx context.Context, cl client.Client, nodeLabels nodelabels.Cache, affinity *grovecorev1alpha1.TopologyAffinity) (string, []string, error) {
	if affinity == nil {
		return "", nil, nil
	}
	labelKey, err := KeyForDomain(ctx, cl, affinity.TopologyName, grovecorev1alpha1.TopologyDomain(affinity.Domain))
	if err != nil {
		return "", nil, err
	}
	values, err := nodeLabels.Values(ctx, labelKey)
	if err != nil {
		return "", nil, err
	}
	return labelKey, values, nil
}
