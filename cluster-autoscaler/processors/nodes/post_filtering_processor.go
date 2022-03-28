/*
Copyright 2021 The Kubernetes Authors.

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
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator"
	"k8s.io/klog/v2"
	"sort"
)

// PostFilteringScaleDownNodeProcessor selects first maxCount nodes (if possible) to be removed
type PostFilteringScaleDownNodeProcessor struct {
}

type ZoneNodeCount struct {
	Zone  string
	Count int
}

// GetNodesToRemove selects up to maxCount nodes for deletion, balancing out zone distribution of nodes
func (n *PostFilteringScaleDownNodeProcessor) GetNodesToRemove(ctx *context.AutoscalingContext, candidates []simulator.NodeToBeRemoved, maxCount int) []simulator.NodeToBeRemoved {
	allNodes, err := ctx.ReadyNodeLister().List()
	if err != nil {
		klog.Warningf("Can't retrieve ready nodes to determine zone balancing. Removing nodes without respecting zones", err)
		return getFirstNodesToRemove(candidates, maxCount)
	}
	zoneCount := make(map[string]int)
	for _, node := range allNodes {
		zone, found := node.Annotations["topology.kubernetes.io/zone"]
		if found {
			zoneCount[zone] += 1
		}
	}
	if len(zoneCount) == 0 {
		// no zone information available
		return getFirstNodesToRemove(candidates, maxCount)
	}
	removedNodes := make([]simulator.NodeToBeRemoved, 0)
	removeCount := maxCount
	sortedZoneCounts := sortedZoneCounts(&zoneCount)
	if maxCount > len(candidates) {
		removeCount = len(candidates)
	}
	candidatesLeft := true
	for len(removedNodes) < removeCount && candidatesLeft {
		for _, count := range sortedZoneCounts {
			// for each zone try to find a node that can be removed, starting with the largest zone
			candidate := findNodeWithZone(candidates, count.Zone, removedNodes)
			if candidate != nil {
				removedNodes = append(removedNodes, *candidate)
				count.Count--
				// this iteration yielded one candidate, so there could be more
				candidatesLeft = true
				break
			}
			// no zone has any candidates left
			candidatesLeft = false
		}
		sortZoneCounts(sortedZoneCounts)
	}
	return removedNodes
}

// getFirstNodesToRemove selects a first maxCount nodes from the candidates
func getFirstNodesToRemove(candidates []simulator.NodeToBeRemoved, maxCount int) []simulator.NodeToBeRemoved {
	end := len(candidates)
	if len(candidates) > maxCount {
		end = maxCount
	}
	return candidates[:end]
}

func sortedZoneCounts(zoneCount *map[string]int) []ZoneNodeCount {
	result := make([]ZoneNodeCount, len(*zoneCount))
	for key, value := range *zoneCount {
		result = append(result, ZoneNodeCount{Count: value, Zone: key})
	}
	sortZoneCounts(result)
	return result
}

func sortZoneCounts(zoneCount []ZoneNodeCount) {
	sort.Slice(zoneCount, func(i, j int) bool {
		return zoneCount[i].Count > zoneCount[j].Count
	})
}

func findNodeWithZone(candidates []simulator.NodeToBeRemoved, zone string, removedNodes []simulator.NodeToBeRemoved) *simulator.NodeToBeRemoved {
	for _, candidate := range candidates {
		if contains(removedNodes, candidate) || !isInZone(candidate, zone) {
			continue
		}
		return &candidate
	}
	return nil
}

func contains(s []simulator.NodeToBeRemoved, e simulator.NodeToBeRemoved) bool {
	for _, a := range s {
		if a.Node.Name == e.Node.Name {
			return true
		}
	}
	return false
}

func isInZone(node simulator.NodeToBeRemoved, zone string) bool {
	nodeZone, found := node.Node.Annotations["topology.kubernetes.io/zone"]
	if !found {
		return false
	}
	return nodeZone == zone
}

// CleanUp is called at CA termination
func (n *PostFilteringScaleDownNodeProcessor) CleanUp() {
}

// NewPostFilteringScaleDownNodeProcessor returns a new PostFilteringScaleDownNodeProcessor
func NewPostFilteringScaleDownNodeProcessor() *PostFilteringScaleDownNodeProcessor {
	return &PostFilteringScaleDownNodeProcessor{}
}
