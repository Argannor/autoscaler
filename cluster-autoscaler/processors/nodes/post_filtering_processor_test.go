/*
Copyright 2019 The Kubernetes Authors.

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

package nodes

import (
	"k8s.io/autoscaler/cluster-autoscaler/simulator"
	"k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"

	"k8s.io/autoscaler/cluster-autoscaler/context"
	. "k8s.io/autoscaler/cluster-autoscaler/utils/test"
)

type testNodeLister struct {
	list []*apiv1.Node
}

func (n *testNodeLister) List() ([]*apiv1.Node, error) {
	return n.list, nil
}

func (n *testNodeLister) Get(name string) (*apiv1.Node, error) {
	return nil, nil
}

func newTestAutoscalingContext(readyNodes []*apiv1.Node) context.AutoscalingContext {
	readyNodeLister := testNodeLister{
		list: readyNodes,
	}
	return context.AutoscalingContext{
		AutoscalingKubeClients: context.AutoscalingKubeClients{
			ListerRegistry: kubernetes.NewListerRegistry(nil, &readyNodeLister, nil, nil, nil, nil, nil, nil, nil, nil),
		},
	}
}

func TestPostFilteringScaleDownNodeProcessor_GetNodesToRemove(t *testing.T) {
	n1 := simulator.NodeToBeRemoved{
		Node:             BuildTestNode("n1", 100, 1000),
		PodsToReschedule: []*apiv1.Pod{},
		DaemonSetPods:    []*apiv1.Pod{},
	}
	n2 := simulator.NodeToBeRemoved{
		Node:             BuildTestNode("n2", 100, 1000),
		PodsToReschedule: []*apiv1.Pod{},
		DaemonSetPods:    []*apiv1.Pod{},
	}

	processor := NewPostFilteringScaleDownNodeProcessor()
	expectedNodes := []simulator.NodeToBeRemoved{n1}
	candidates := []simulator.NodeToBeRemoved{n1, n2}
	ctx := newTestAutoscalingContext([]*apiv1.Node{})
	nodes := processor.GetNodesToRemove(&ctx, candidates, 1)

	assert.Equal(t, expectedNodes, nodes)
}

func TestPostFilteringScaleDownNodeProcessor_GetNodesToRemoveWithZones(t *testing.T) {
	n1 := simulator.NodeToBeRemoved{
		Node:             buildTestNode("n1", 100, 1000, "A"),
		PodsToReschedule: []*apiv1.Pod{},
		DaemonSetPods:    []*apiv1.Pod{},
	}
	n2 := simulator.NodeToBeRemoved{
		Node:             buildTestNode("n2", 100, 1000, "B"),
		PodsToReschedule: []*apiv1.Pod{},
		DaemonSetPods:    []*apiv1.Pod{},
	}
	n3 := simulator.NodeToBeRemoved{
		Node:             buildTestNode("n3-not-removable", 100, 1000, "B"),
		PodsToReschedule: []*apiv1.Pod{},
		DaemonSetPods:    []*apiv1.Pod{},
	}

	processor := NewPostFilteringScaleDownNodeProcessor()
	expectedNodes := []simulator.NodeToBeRemoved{n2}
	candidates := []simulator.NodeToBeRemoved{n1, n2}
	ctx := newTestAutoscalingContext([]*apiv1.Node{n1.Node, n2.Node, n3.Node})
	nodes := processor.GetNodesToRemove(&ctx, candidates, 1)

	assert.Equal(t, expectedNodes, nodes)
}

func TestPostFilteringScaleDownNodeProcessor_GetNodesToRemoveWithZonesUnfulfillable(t *testing.T) {
	n1 := simulator.NodeToBeRemoved{
		Node:             buildTestNode("n1", 100, 1000, "A"),
		PodsToReschedule: []*apiv1.Pod{},
		DaemonSetPods:    []*apiv1.Pod{},
	}
	n2 := simulator.NodeToBeRemoved{
		Node:             buildTestNode("n2", 100, 1000, "B"),
		PodsToReschedule: []*apiv1.Pod{},
		DaemonSetPods:    []*apiv1.Pod{},
	}
	n3 := simulator.NodeToBeRemoved{
		Node:             buildTestNode("n3-not-removable", 100, 1000, "B"),
		PodsToReschedule: []*apiv1.Pod{},
		DaemonSetPods:    []*apiv1.Pod{},
	}

	processor := NewPostFilteringScaleDownNodeProcessor()
	expectedNodes := []simulator.NodeToBeRemoved{n2, n1}
	candidates := []simulator.NodeToBeRemoved{n1, n2}
	ctx := newTestAutoscalingContext([]*apiv1.Node{n1.Node, n2.Node, n3.Node})
	nodes := processor.GetNodesToRemove(&ctx, candidates, 3)

	assert.Equal(t, expectedNodes, nodes)
}

// ignore nodes that have no zone information, if some nodes have zone information
func TestPostFilteringScaleDownNodeProcessor_GetNodesToRemoveWithMissingZonesOnSomeNodes(t *testing.T) {
	n1 := simulator.NodeToBeRemoved{
		Node:             buildTestNode("n1", 100, 1000, "A"),
		PodsToReschedule: []*apiv1.Pod{},
		DaemonSetPods:    []*apiv1.Pod{},
	}
	n2 := simulator.NodeToBeRemoved{
		Node:             buildTestNode("n2", 100, 1000, "B"),
		PodsToReschedule: []*apiv1.Pod{},
		DaemonSetPods:    []*apiv1.Pod{},
	}
	n3 := simulator.NodeToBeRemoved{
		Node:             buildTestNode("n3-not-removable", 100, 1000, "B"),
		PodsToReschedule: []*apiv1.Pod{},
		DaemonSetPods:    []*apiv1.Pod{},
	}
	n4 := simulator.NodeToBeRemoved{
		Node:             BuildTestNode("n4-no-zone", 100, 1000),
		PodsToReschedule: []*apiv1.Pod{},
		DaemonSetPods:    []*apiv1.Pod{},
	}

	processor := NewPostFilteringScaleDownNodeProcessor()
	expectedNodes := []simulator.NodeToBeRemoved{n2, n1}
	candidates := []simulator.NodeToBeRemoved{n1, n2, n4}
	ctx := newTestAutoscalingContext([]*apiv1.Node{n1.Node, n2.Node, n3.Node})
	nodes := processor.GetNodesToRemove(&ctx, candidates, 3)

	assert.Equal(t, expectedNodes, nodes)
}

func buildTestNode(name string, millicpu int64, mem int64, zone string) *apiv1.Node {
	node := BuildTestNode(name, millicpu, mem)
	node.Annotations = make(map[string]string)
	node.Annotations["topology.kubernetes.io/zone"] = zone
	return node
}
