/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package integration_test

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/kwok/apis/v1alpha1"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/test"
)

// expendableCutoffEnv is the setting under test. Pods whose priority is
// strictly below its value are expendable: Karpenter does not provision
// capacity for them and does not require them to reschedule during disruption.
const expendableCutoffEnv = "EXPENDABLE_PODS_PRIORITY_CUTOFF"

// These specs need the cluster's non-Karpenter nodes to be unschedulable for
// test pods (for kind, taint the control plane with CriticalAddonsOnly per the
// kwok README). Otherwise expendable pods land there and "no NodeClaim" proves
// nothing.
var _ = Describe("ExpendablePods", func() {
	var expendable, atCutoff *schedulingv1.PriorityClass

	BeforeEach(func() {
		// Never lets an expendable pod preempt anything. The class used
		// throughout sets it deliberately, to prove preemptionPolicy does not
		// affect whether a pod is expendable.
		expendable = &schedulingv1.PriorityClass{
			ObjectMeta:       metav1.ObjectMeta{Name: fmt.Sprintf("expendable-%s", test.RandomName())},
			Value:            -100,
			PreemptionPolicy: lo.ToPtr(corev1.PreemptNever),
		}
		// Exactly at the cutoff: the lowest priority that still gets
		// capacity provisioned.
		atCutoff = &schedulingv1.PriorityClass{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("at-cutoff-%s", test.RandomName())},
			Value:      -10,
		}
		// Pin the instance size so "fits in the spare capacity" is
		// predictable: one 1-CPU pod leaves room for a couple more on a 4-CPU
		// node, and never for six.
		if env.IsDefaultNodeClassKWOK() {
			test.ReplaceRequirements(nodePool, v1.NodeSelectorRequirementWithMinValues{
				Key:      v1alpha1.InstanceCPULabelKey,
				Operator: corev1.NodeSelectorOpIn,
				Values:   []string{"4"},
			})
		}
	})

	// Ordered so the setting is changed (and Karpenter restarted) once for the
	// group; ContinueOnFailure so one failing spec does not skip the rest.
	Context("with the cutoff set to -10", Ordered, ContinueOnFailure, func() {
		BeforeAll(func() {
			original := env.ExpectSettings()
			DeferCleanup(func() { env.ExpectSettingsReplaced(original...) })
			env.ExpectSettingsOverridden(corev1.EnvVar{Name: expendableCutoffEnv, Value: "-10"})
		})

		Context("Provisioning", func() {
			It("should not provision capacity for pods below the cutoff", func() {
				dep := expendableTestDeployment("below-cutoff", expendable.Name, 3, nil)
				env.ExpectCreated(nodeClass, nodePool, expendable, dep)

				env.EventuallyExpectPendingPodCount(expendableTestSelector(dep), 3)
				env.ConsistentlyExpectNodeClaimCountNotExceed(time.Minute, 0)
			})
			It("should provision capacity for pods exactly at the cutoff", func() {
				dep := expendableTestDeployment("at-cutoff", atCutoff.Name, 3, nil)
				env.ExpectCreated(nodeClass, nodePool, atCutoff, dep)

				env.EventuallyExpectCreatedNodeClaimCount(">=", 1)
				env.EventuallyExpectHealthyPodCount(expendableTestSelector(dep), 3)
			})
			It("should provision capacity for pods at the default priority", func() {
				dep := expendableTestDeployment("default-priority", "", 3, nil)
				env.ExpectCreated(nodeClass, nodePool, dep)

				env.EventuallyExpectCreatedNodeClaimCount(">=", 1)
				env.EventuallyExpectHealthyPodCount(expendableTestSelector(dep), 3)
			})
			It("should place expendable pods in spare capacity without launching nodes for the rest", func() {
				nodePool.Spec.Disruption.ConsolidateAfter = v1.MustParseNillableDuration("Never")
				normal := expendableTestDeployment("normal", "", 1, nil)
				env.ExpectCreated(nodeClass, nodePool, expendable, normal)
				env.EventuallyExpectCreatedNodeClaimCount("==", 1)
				env.EventuallyExpectHealthyPodCount(expendableTestSelector(normal), 1)

				// Six 1-CPU pods cannot all fit beside the normal pod on one
				// 4-CPU node: some land in the spare capacity, the rest wait.
				backfill := expendableTestDeployment("backfill", expendable.Name, 6, nil)
				env.ExpectCreated(backfill)

				eventuallyExpectBoundAndPending(expendableTestSelector(backfill), 1, 1)
				env.ConsistentlyExpectNodeClaimCountNotExceed(time.Minute, 1)
			})
		})

		Context("Disruption", func() {
			var normal, backfill *appsv1.Deployment

			// launchSharedNode puts one normal pod and two expendable pods on a
			// single Karpenter node, then removes the normal pod so only the
			// expendable ones remain.
			launchSharedNode := func(backfillAnnotations map[string]string) *v1.NodeClaim {
				GinkgoHelper()
				normal = expendableTestDeployment("normal", "", 1, nil)
				backfill = expendableTestDeployment("backfill", expendable.Name, 2, backfillAnnotations)

				env.ExpectCreated(nodeClass, nodePool, expendable, normal)
				nodeClaim := env.EventuallyExpectCreatedNodeClaimCount("==", 1)[0]
				env.EventuallyExpectHealthyPodCount(expendableTestSelector(normal), 1)

				env.ExpectCreated(backfill)
				env.EventuallyExpectHealthyPodCount(expendableTestSelector(backfill), 2)

				env.ExpectDeleted(normal)
				return nodeClaim
			}

			It("should consolidate a node running only expendable pods as empty", func() {
				nodePool.Spec.Disruption.ConsolidationPolicy = v1.ConsolidationPolicyWhenEmpty
				nodePool.Spec.Disruption.ConsolidateAfter = v1.MustParseNillableDuration("0s")
				nodeClaim := launchSharedNode(nil)

				env.EventuallyExpectConsolidatable(nodeClaim)
				// Bounded: without the cutoff the node is never empty, and the
				// suite's 15-minute default would make that failure very slow.
				env.EventuallyExpectNotFoundAssertion(nodeClaim).WithTimeout(3 * time.Minute).Should(Succeed())
				env.ConsistentlyExpectNodeClaimCountNotExceed(time.Minute, 0)
				env.EventuallyExpectPendingPodCount(expendableTestSelector(backfill), 2)
			})
			It("should not replace capacity for expendable pods when their node is drained", func() {
				nodePool.Spec.Disruption.ConsolidateAfter = v1.MustParseNillableDuration("Never")
				nodeClaim := launchSharedNode(nil)

				env.ExpectDeleted(nodeClaim)

				// Not EventuallyExpectDeletedNodeCount: that counts nodes present
				// at the monitor's reset in BeforeEach, and this node came later.
				env.EventuallyExpectNotFound(nodeClaim)
				// Check for a replacement first: without the cutoff one is
				// launched at once, and the evicted pods run on it, so a pending
				// check would only fail after the 15-minute default.
				env.ConsistentlyExpectNodeClaimCountNotExceed(time.Minute, 0)
				env.EventuallyExpectPendingPodCount(expendableTestSelector(backfill), 2)
			})
			It("should not consolidate a node when an expendable pod has do-not-disrupt", func() {
				nodePool.Spec.Disruption.ConsolidationPolicy = v1.ConsolidationPolicyWhenEmpty
				nodePool.Spec.Disruption.ConsolidateAfter = v1.MustParseNillableDuration("0s")
				nodeClaim := launchSharedNode(map[string]string{v1.DoNotDisruptAnnotationKey: "true"})

				env.EventuallyExpectConsolidatable(nodeClaim)
				env.ConsistentlyExpectNoDisruptions(1, time.Minute)
			})
		})
	})

	// Controls: the same pods with the setting absent. They prove the specs
	// above can fail, i.e. that "no NodeClaim" comes from the cutoff and not
	// from a NodePool that could never satisfy these pods.
	Context("without the cutoff", Ordered, ContinueOnFailure, func() {
		BeforeAll(func() {
			original := env.ExpectSettings()
			DeferCleanup(func() { env.ExpectSettingsReplaced(original...) })
			env.ExpectSettingsRemoved(corev1.EnvVar{Name: expendableCutoffEnv})
		})

		It("should provision capacity for low-priority pods", func() {
			dep := expendableTestDeployment("below-cutoff", expendable.Name, 3, nil)
			env.ExpectCreated(nodeClass, nodePool, expendable, dep)

			env.EventuallyExpectCreatedNodeClaimCount(">=", 1)
			env.EventuallyExpectHealthyPodCount(expendableTestSelector(dep), 3)
		})
		It("should not treat a node running only low-priority pods as empty", func() {
			nodePool.Spec.Disruption.ConsolidationPolicy = v1.ConsolidationPolicyWhenEmpty
			nodePool.Spec.Disruption.ConsolidateAfter = v1.MustParseNillableDuration("0s")
			normal := expendableTestDeployment("normal", "", 1, nil)
			backfill := expendableTestDeployment("backfill", expendable.Name, 2, nil)

			env.ExpectCreated(nodeClass, nodePool, expendable, normal)
			nodeClaim := env.EventuallyExpectCreatedNodeClaimCount("==", 1)[0]
			env.EventuallyExpectHealthyPodCount(expendableTestSelector(normal), 1)
			env.ExpectCreated(backfill)
			env.EventuallyExpectHealthyPodCount(expendableTestSelector(backfill), 2)
			env.ExpectDeleted(normal)

			env.EventuallyExpectConsolidatable(nodeClaim)
			env.ConsistentlyExpectNoDisruptions(1, time.Minute)
		})
	})
})

// expendableTestDeployment builds a deployment of 1-CPU pods labeled app=name.
// An empty priorityClass leaves the pods at the cluster's default priority.
func expendableTestDeployment(name, priorityClass string, replicas int32, annotations map[string]string) *appsv1.Deployment {
	return test.Deployment(test.DeploymentOptions{
		Replicas: replicas,
		PodOptions: test.PodOptions{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      map[string]string{"app": name},
				Annotations: annotations,
			},
			PriorityClassName: priorityClass,
			ResourceRequirements: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
			},
		},
	})
}

// expendableTestSelector selects the pods of a deployment built by expendableTestDeployment.
func expendableTestSelector(dep *appsv1.Deployment) labels.Selector {
	return labels.SelectorFromSet(dep.Spec.Selector.MatchLabels)
}

// eventuallyExpectBoundAndPending waits until at least minBound of the selected
// pods are bound to a node and at least minPending are still unbound.
func eventuallyExpectBoundAndPending(selector labels.Selector, minBound, minPending int) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		pods := &corev1.PodList{}
		g.Expect(env.Client.List(env, pods, client.MatchingLabelsSelector{Selector: selector})).To(Succeed())
		bound := lo.CountBy(pods.Items, func(p corev1.Pod) bool { return p.Spec.NodeName != "" })
		g.Expect(bound).To(BeNumerically(">=", minBound))
		g.Expect(len(pods.Items) - bound).To(BeNumerically(">=", minPending))
	}).Should(Succeed())
}
