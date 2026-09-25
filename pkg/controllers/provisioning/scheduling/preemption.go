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

package scheduling

import (
	"context"
	"math"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/log"

	karpopts "sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/utils/pod"
)

// canPreemptExpendable returns whether the scheduler may place the pod in the room that expendable pods hold on existing
// nodes, because the kube-scheduler will preempt them for it. That's true for pods moved off a disrupted node and for
// CapacityBuffer virtual pods, but not for pending pods: a pending pod reaches Karpenter only after the kube-scheduler
// has declined to preempt for it, so treating the room as free would leave it pending.
func (s *Scheduler) canPreemptExpendable(ctx context.Context, p *corev1.Pod) bool {
	cutoff := karpopts.FromContext(ctx).ExpendablePodsPriorityCutoff
	// No pod is expendable, so there's no room to preempt
	if cutoff == math.MinInt32 {
		return false
	}
	if isVirtualBufferPod(p) {
		return s.bufferCanPreemptExpendable(ctx, p, cutoff)
	}
	return p.Spec.NodeName != "" && pod.CanPreemptExpendable(p, cutoff)
}

// bufferCanPreemptExpendable returns whether pods created from a CapacityBuffer's template could preempt expendable pods.
// Virtual pods run at the lowest priority, so this resolves the priority and preemption policy that the template's pods
// will be admitted with from the template's PriorityClass instead.
func (s *Scheduler) bufferCanPreemptExpendable(ctx context.Context, p *corev1.Pod, cutoff int32) bool {
	if p.Spec.PreemptionPolicy != nil && *p.Spec.PreemptionPolicy == corev1.PreemptNever {
		return false
	}
	if canPreempt, ok := s.cachedBufferPreemption[p.Spec.PriorityClassName]; ok {
		return canPreempt
	}
	canPreempt := false
	if priorityClass, err := s.admittedPriorityClass(ctx, p.Spec.PriorityClassName); err != nil {
		// The template's pods can't be admitted without their PriorityClass, so they won't preempt anything
		log.FromContext(ctx).V(1).WithValues("PriorityClass", klog.KRef("", p.Spec.PriorityClassName)).Info("failed resolving capacity buffer priority class", "error", err.Error())
	} else {
		// Without a PriorityClass, pods are admitted at priority 0 and may preempt lower priority pods
		templatePod := &corev1.Pod{Spec: corev1.PodSpec{Priority: lo.ToPtr(int32(0))}}
		if priorityClass != nil {
			templatePod.Spec.Priority = lo.ToPtr(priorityClass.Value)
			templatePod.Spec.PreemptionPolicy = priorityClass.PreemptionPolicy
		}
		canPreempt = pod.CanPreemptExpendable(templatePod, cutoff)
	}
	if s.cachedBufferPreemption == nil {
		s.cachedBufferPreemption = map[string]bool{}
	}
	s.cachedBufferPreemption[p.Spec.PriorityClassName] = canPreempt
	return canPreempt
}

// admittedPriorityClass returns the PriorityClass that a pod naming priorityClassName is admitted with. A pod that names
// none gets the lowest-valued global default PriorityClass, or none (nil) if there isn't one.
func (s *Scheduler) admittedPriorityClass(ctx context.Context, priorityClassName string) (*schedulingv1.PriorityClass, error) {
	if priorityClassName != "" {
		priorityClass := &schedulingv1.PriorityClass{}
		if err := s.kubeClient.Get(ctx, types.NamespacedName{Name: priorityClassName}, priorityClass); err != nil {
			return nil, err
		}
		return priorityClass, nil
	}
	priorityClasses := &schedulingv1.PriorityClassList{}
	if err := s.kubeClient.List(ctx, priorityClasses); err != nil {
		return nil, err
	}
	var globalDefault *schedulingv1.PriorityClass
	for i := range priorityClasses.Items {
		if pc := &priorityClasses.Items[i]; pc.GlobalDefault && (globalDefault == nil || pc.Value < globalDefault.Value) {
			globalDefault = pc
		}
	}
	return globalDefault, nil
}
