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

package internal

import (
	"context"
	"sync"
	"time"

	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	groveclientset "github.com/ai-dynamo/grove/operator/client/clientset/versioned"
	groveinformers "github.com/ai-dynamo/grove/operator/client/informers/externalversions"
	groveerr "github.com/ai-dynamo/grove/operator/internal/errors"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

// Constants for error codes.
const (
	errCodeClientCreation       grovecorev1alpha1.ErrorCode = "ERR_CLIENT_CREATION"
	errCodeRegisterEventHandler grovecorev1alpha1.ErrorCode = "ERR_REGISTER_EVENT_HANDLER"
	errCodeCacheSync            grovecorev1alpha1.ErrorCode = "ERR_CACHE_SYNC"
)

const (
	operationWaitForParentPodClique = "WaitForParentPodClique"
)

// ParentPodCliqueDependencies contains the last known (readiness) state of all parent PodCliques.
type ParentPodCliqueDependencies struct {
	namespace string // Kubernetes namespace to watch for PodCliques

	pclqFQNs          sets.Set[string] // PodCliques whose readyReplicas must satisfy spec.minAvailable
	currentReadyPCLQs sets.Set[string] // PodCliques currently satisfying their readiness dependency

	pclqFQNToConditions   map[string]sets.Set[string] // Required conditions per PodClique
	currentPCLQConditions map[string]sets.Set[string] // Currently true required conditions per PodClique

	ready chan struct{} // Signals when all dependencies are satisfied

	mu sync.Mutex
}

// NewPodCliqueState creates and initializes all parent PodCliques with an unready state.
func NewPodCliqueState(podCliqueDependencies sets.Set[string], podCliqueConditionDependencies map[string][]string, namespace string) *ParentPodCliqueDependencies {
	requiredConditions := make(map[string]sets.Set[string])
	currentlyTrueConditions := make(map[string]sets.Set[string])
	for podCliqueName, conditions := range podCliqueConditionDependencies {
		requiredConditions[podCliqueName] = sets.New(conditions...)
		currentlyTrueConditions[podCliqueName] = sets.New[string]()
	}

	return &ParentPodCliqueDependencies{
		namespace:             namespace,
		pclqFQNs:              podCliqueDependencies,
		pclqFQNToConditions:   requiredConditions,
		currentReadyPCLQs:     sets.New[string](),
		currentPCLQConditions: currentlyTrueConditions,
		ready:                 make(chan struct{}, 1),
	}
}

// WaitForReady waits for all upstream start-up dependencies to be ready.
func (c *ParentPodCliqueDependencies) WaitForReady(ctx context.Context, log logr.Logger) error {
	defer close(c.ready) // Close the channel the informers write to *after* the context they use is cancelled.

	if c.pclqFQNs.Len() == 0 && len(c.pclqFQNToConditions) == 0 {
		return nil
	}

	log.Info("Parent PodClique(s) being waited on",
		"pclqFQNs", c.pclqFQNs,
		"pclqFQNToConditions", c.pclqFQNToConditions)

	restConfig, err := createRestConfig()
	if err != nil {
		return err
	}

	groveClient, err := createGroveClient(restConfig)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	podCliqueFactory := groveinformers.NewSharedInformerFactoryWithOptions(
		groveClient,
		time.Second,
		groveinformers.WithNamespace(c.namespace),
	)

	defer func() {
		cancel() // Cancel the context used by the informers if the wait is successful, or an err occurs.
		podCliqueFactory.Shutdown()
	}()

	podCliqueInformer := podCliqueFactory.Grove().V1alpha1().PodCliques().Informer()
	if err := c.registerPodCliqueEventHandler(podCliqueInformer, log); err != nil {
		return groveerr.WrapError(
			err,
			errCodeRegisterEventHandler,
			operationWaitForParentPodClique,
			"failed to register the PodClique event handler",
		)
	}

	podCliqueFactory.Start(ctx.Done())
	for _, synced := range podCliqueFactory.WaitForCacheSync(ctx.Done()) {
		if !synced {
			return groveerr.New(errCodeCacheSync, operationWaitForParentPodClique, "failed to sync informer cache")
		}
	}

	select {
	case <-c.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func createRestConfig() (*rest.Config, error) {
	restConfig, err := rest.InClusterConfig()
	if err != nil {
		return nil, groveerr.WrapError(
			err,
			errCodeClientCreation,
			operationWaitForParentPodClique,
			"failed to fetch the in cluster config",
		)
	}
	return restConfig, nil
}

func createGroveClient(restConfig *rest.Config) (groveclientset.Interface, error) {
	client, err := groveclientset.NewForConfig(restConfig)
	if err != nil {
		return nil, groveerr.WrapError(
			err,
			errCodeClientCreation,
			operationWaitForParentPodClique,
			"failed to create Grove client with the fetched restConfig",
		)
	}
	return client, nil
}

func (c *ParentPodCliqueDependencies) registerPodCliqueEventHandler(informer cache.SharedIndexInformer, log logr.Logger) error {
	_, err := informer.AddEventHandlerWithOptions(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			c.refreshPodClique(obj.(*grovecorev1alpha1.PodClique))
		},
		UpdateFunc: func(_, newObj any) {
			c.refreshPodClique(newObj.(*grovecorev1alpha1.PodClique))
		},
		DeleteFunc: func(obj any) {
			pclq, err := cache.DeletionHandlingObjectToName(obj)
			if err != nil {
				log.Error(err, "failed to read PodClique name from deletion event")
				return
			}

			c.mu.Lock()
			defer c.mu.Unlock()

			c.currentReadyPCLQs.Delete(pclq.Name)
			delete(c.currentPCLQConditions, pclq.Name)

			c.notifyIfAllParentsReady()
		},
	}, cache.HandlerOptions{Logger: &log})
	return err
}

func (c *ParentPodCliqueDependencies) refreshPodClique(pclq *grovecorev1alpha1.PodClique) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.refreshReadinessOfPodClique(pclq)
	c.refreshConditionsOfPodClique(pclq)
	c.notifyIfAllParentsReady()
}

func (c *ParentPodCliqueDependencies) refreshReadinessOfPodClique(pclq *grovecorev1alpha1.PodClique) {
	podCliqueName := pclq.GetName()
	if !c.pclqFQNs.Has(podCliqueName) {
		return
	}

	requiredReplicas, ok := pclq.MinAvailable()
	if ok && pclq.Status.ReadyReplicas >= requiredReplicas {
		c.currentReadyPCLQs.Insert(podCliqueName)
	} else {
		c.currentReadyPCLQs.Delete(podCliqueName)
	}
}

func (c *ParentPodCliqueDependencies) refreshConditionsOfPodClique(pclq *grovecorev1alpha1.PodClique) {
	podCliqueName := pclq.GetName()
	requiredConditions, ok := c.pclqFQNToConditions[podCliqueName]
	if !ok {
		return
	}

	trueConditions := sets.New[string]()
	for _, condition := range pclq.Status.Conditions {
		if requiredConditions.Has(condition.Type) && condition.Status == metav1.ConditionTrue {
			trueConditions.Insert(condition.Type)
		}
	}
	c.currentPCLQConditions[podCliqueName] = trueConditions
}

// notifyIfAllParentsReady notifies consumers if all PodClique readiness and condition dependencies are true.
func (c *ParentPodCliqueDependencies) notifyIfAllParentsReady() {
	for cliqueName := range c.pclqFQNs {
		if !c.currentReadyPCLQs.Has(cliqueName) {
			return
		}
	}

	for cliqueName, requiredConditions := range c.pclqFQNToConditions {
		trueConditions := c.currentPCLQConditions[cliqueName]
		if !trueConditions.HasAll(sets.List(requiredConditions)...) {
			return
		}
	}

	select {
	case c.ready <- struct{}{}:
	default: // already notified
	}
}
