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
	"fmt"
	"strings"

	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	groveerr "github.com/ai-dynamo/grove/operator/internal/errors"

	"github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/util/sets"
)

// Constants for error codes.
const (
	errCodeInvalidInput grovecorev1alpha1.ErrorCode = "ERR_INVALID_INPUT"
)

const (
	operationParseFlag = "OperationParseFlag"
)

// CLIOptions defines the configuration that is passed to the init container.
type CLIOptions struct {
	podCliques []string // PodClique names with an optional condition in format "name[:condition]"
}

// RegisterFlags registers all the flags that are defined for the init container.
func (c *CLIOptions) RegisterFlags() {
	// --podcliques=<podclique-fqn>[:<condition-type>]
	// --podcliques=podclique-a --podcliques=podclique-b:TopologyAffinityReady and so on for each PodClique.
	pflag.StringArrayVarP(&c.podCliques, "podcliques", "p", nil, "podclique name and optional condition type separated by colon, repeated for each podclique")
}

// GetPodCliqueDependencyConfig returns readiness and condition dependencies parsed from all initc flags.
func (c *CLIOptions) GetPodCliqueDependencyConfig() (sets.Set[string], map[string][]string, error) {
	podCliqueDependencies := sets.New[string]()
	podCliqueConditionDependencies := make(map[string][]string)
	for _, pair := range c.podCliques {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}

		parts := strings.Split(pair, ":")
		if len(parts) != 1 && len(parts) != 2 {
			return nil, nil, groveerr.New(errCodeInvalidInput, operationParseFlag, fmt.Sprintf("expected one or two values per podclique, found %d", len(parts)))
		}

		podCliqueName := strings.TrimSpace(parts[0])
		if podCliqueName == "" {
			return nil, nil, groveerr.New(errCodeInvalidInput, operationParseFlag, "podclique name cannot be empty")
		}

		if len(parts) == 1 {
			podCliqueDependencies.Insert(podCliqueName)
		} else {
			conditionType := strings.TrimSpace(parts[1])
			if conditionType == "" {
				return nil, nil, groveerr.New(errCodeInvalidInput, operationParseFlag, "podclique condition type cannot be empty")
			}
			podCliqueConditionDependencies[podCliqueName] = append(podCliqueConditionDependencies[podCliqueName], conditionType)
		}
	}
	return podCliqueDependencies, podCliqueConditionDependencies, nil
}

// InitializeCLIOptions parses the command line flags into CLIOptions.
func InitializeCLIOptions() (CLIOptions, error) {
	config := CLIOptions{
		podCliques: make([]string, 0),
	}
	config.RegisterFlags()
	pflag.Parse()
	return config, nil
}
