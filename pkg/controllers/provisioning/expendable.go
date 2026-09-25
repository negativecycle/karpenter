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

package provisioning

import (
	"context"
	"math"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/utils/pod"
)

// FilterExpendable splits pods into those Karpenter should provision (or size replacements) for and those below the
// expendable pods priority cutoff. When the cutoff is the default, kept is pods itself.
func FilterExpendable(ctx context.Context, pods []*corev1.Pod) (kept, expendable []*corev1.Pod) {
	cutoff := options.FromContext(ctx).ExpendablePodsPriorityCutoff
	// No pod's priority can be below the default cutoff, so don't allocate when the feature is off
	if cutoff == math.MinInt32 {
		return pods, nil
	}
	expendable, kept = lo.FilterReject(pods, func(p *corev1.Pod, _ int) bool { return pod.IsExpendable(p, cutoff) })
	return kept, expendable
}
