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

package v1alpha1

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/utils/ptr"
)

func TestPodCliqueMinAvailable(t *testing.T) {
	tests := []struct {
		name      string
		pclq      *PodClique
		want      int32
		wantKnown bool
	}{
		{
			name: "normal podclique uses minAvailable",
			pclq: &PodClique{
				Spec: PodCliqueSpec{
					Replicas:     3,
					MinAvailable: ptr.To[int32](2),
				},
			},
			want:      2,
			wantKnown: true,
		},
		{
			name: "normal podclique falls back to replicas",
			pclq: &PodClique{
				Spec: PodCliqueSpec{
					Replicas: 3,
				},
			},
			want:      3,
			wantKnown: true,
		},
		{
			name:      "topology affinity expands replicas across target domains",
			pclq:      topologyAffinityPodClique(2, []string{"rack-a", "rack-b", "rack-c"}),
			want:      6,
			wantKnown: true,
		},
		{
			name:      "topology affinity waits until status is available",
			pclq:      topologyAffinityPodClique(2, nil),
			wantKnown: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, known := tt.pclq.MinAvailable()

			assert.Equal(t, tt.wantKnown, known)
			assert.Equal(t, tt.want, got)
		})
	}
}

func topologyAffinityPodClique(replicas int32, targetDomains []string) *PodClique {
	pclq := &PodClique{
		Spec: PodCliqueSpec{
			Replicas:     replicas,
			MinAvailable: ptr.To(replicas),
			Affinity: &PodCliqueAffinity{
				TopologyAffinity: &TopologyAffinity{
					TopologyName: "fabric",
					Domain:       "rack",
					CliqueNames:  []string{"parent"},
				},
			},
		},
	}
	if targetDomains != nil {
		pclq.Status.TopologyAffinity = &PodCliqueTopologyAffinityStatus{
			TargetDomains: targetDomains,
		}
	}
	return pclq
}
