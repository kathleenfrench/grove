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

package opts

import (
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/sets"
)

func TestGetPodCliqueDependencyConfig(t *testing.T) {
	options := &CLIOptions{
		podCliques: []string{
			"podclique-a",
			"podclique-a:Ready",
			"podclique-b:TopologyAffinityReady",
			"podclique-c:CustomReady",
		},
	}

	podCliqueDependencies, podCliqueConditionDependencies, err := options.GetPodCliqueDependencyConfig()
	require.NoError(t, err)
	assert.Equal(t, sets.New("podclique-a"), podCliqueDependencies)
	assert.Equal(t, map[string][]string{
		"podclique-a": {"Ready"},
		"podclique-b": {"TopologyAffinityReady"},
		"podclique-c": {"CustomReady"},
	}, podCliqueConditionDependencies)
}

// TestInitializeCLIOptions verifies that CLI options are initialized correctly.
func TestInitializeCLIOptions(t *testing.T) {
	// Reset pflag.CommandLine to avoid interference from other tests
	pflag.CommandLine = pflag.NewFlagSet("test", pflag.ExitOnError)

	config, err := InitializeCLIOptions()

	require.NoError(t, err, "InitializeCLIOptions should not return an error")
	assert.Equal(t, 0, len(config.podCliques), "podCliques slice should be empty initially")
}
