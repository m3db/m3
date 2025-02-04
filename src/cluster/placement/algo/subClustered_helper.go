package algo

import (
	"container/heap"
	"errors"
	"fmt"
	"github.com/m3db/m3/src/cluster/placement"
	"go.uber.org/zap"
	"math"
	"sort"
)

type subClusterHelper struct {
	currLoad             map[uint32]int
	targetLoad           map[uint32]int
	subClusterToShardMap map[uint32][]uint32
	subClusters          []uint32
	rf                   int
	uniqueShards         []uint32
	instances            map[string]placement.Instance
	log                  *zap.Logger
	opts                 placement.Options
	totalWeight          uint32
	maxShardSetID        uint32
}

type shardMoveInfo struct {
	shardIds  []uint32
	movedFrom uint32
}

func newSubClusterInitHelper(subClusterToInstanceMapping map[uint32][]placement.Instance, ids []uint32) (*subClusterHelper, error) {
	subClusterToWeightMap := make(map[uint32]int)
	totalWeight := 0
	h := &subClusterHelper{}
	h.subClusters = make([]uint32, 0)
	for subClusterID, instances := range subClusterToInstanceMapping {
		instanceWeight := 0
		for _, instance := range instances {
			instanceWeight += int(instance.Weight())
		}
		subClusterToWeightMap[subClusterID] = instanceWeight
		totalWeight += instanceWeight
		h.subClusters = append(h.subClusters, subClusterID)
	}
	h.targetLoad = make(map[uint32]int)
	h.currLoad = make(map[uint32]int)
	h.subClusterToShardMap = make(map[uint32][]uint32)
	totalShards := len(ids)
	totalDivided := 0
	for subClusterID, weight := range subClusterToWeightMap {
		h.targetLoad[subClusterID] = int(math.Floor((float64(weight) / float64(totalWeight)) * float64(totalShards)))
		totalDivided += h.targetLoad[subClusterID]
	}
	sort.Sort(UInts(h.subClusters))
	diff := totalShards - totalDivided
	for _, subClusterID := range h.subClusters {
		if diff == 0 {
			break
		}
		h.targetLoad[subClusterID]++
		diff--
	}
	return h, nil

}

func newSubClusterAddHelper(subClusterToInstanceMapping map[uint32][]placement.Instance, totalShards int) (*subClusterHelper, error) {
	subClusterToWeightMap := make(map[uint32]int)
	totalWeight := 0
	h := &subClusterHelper{}
	h.subClusterToShardMap = make(map[uint32][]uint32)
	h.subClusters = make([]uint32, 0)
	h.currLoad = make(map[uint32]int)
	for subClusterID, instances := range subClusterToInstanceMapping {
		instanceWeight := 0
		subClusterToShardMap := make(map[uint32]map[uint32]struct{})
		for _, instance := range instances {
			instanceWeight += int(instance.Weight())
			for _, shardID := range instance.Shards().AllIDs() {
				if _, ok := subClusterToShardMap[subClusterID]; !ok {
					subClusterToShardMap[subClusterID] = make(map[uint32]struct{})
				}
				subClusterToShardMap[subClusterID][shardID] = struct{}{}
			}
		}
		shards := make([]uint32, 0)
		for shardID, _ := range subClusterToShardMap[subClusterID] {
			shards = append(shards, shardID)
		}
		h.subClusterToShardMap[subClusterID] = shards
		h.currLoad[subClusterID] = len(shards)
		sort.Sort(UInts(h.subClusterToShardMap[subClusterID]))
		subClusterToWeightMap[subClusterID] = instanceWeight
		totalWeight += instanceWeight
		h.subClusters = append(h.subClusters, subClusterID)
	}
	sort.Sort(UInts(h.subClusters))
	h.targetLoad = make(map[uint32]int)
	totalDivided := 0
	for subClusterID, weight := range subClusterToWeightMap {
		h.targetLoad[subClusterID] = int(math.Floor((float64(weight) / float64(totalWeight)) * float64(totalShards)))
		totalDivided += h.targetLoad[subClusterID]
	}
	diff := totalShards - totalDivided
	for _, subClusterID := range h.subClusters {
		if diff == 0 {
			break
		}
		h.targetLoad[subClusterID]++
		diff--
	}

	return h, nil
}

func (h *subClusterHelper) balanceShards() (map[uint32][]uint32, map[uint32]map[uint32]shardMoveInfo, error) {
	clusterHeap := newSubClusterHeap(h.subClusters, h.currLoad, h.targetLoad)
	shardsMovedTo := make(map[uint32]map[uint32]shardMoveInfo)
	for i := 0; i < len(h.subClusters); {
		subClusterID := h.subClusters[i]
		for h.targetLoad[subClusterID] > h.currLoad[subClusterID] && clusterHeap.Len() > 0 && i < len(h.subClusters) {
			subCluster := clusterHeap.Pop().(uint32)
			moved, shardId, err := h.moveOneShard(subCluster, subClusterID)
			if err != nil {
				return nil, nil, err
			}
			if moved {
				h.currLoad[subClusterID]++
				clusterHeap.Push(subCluster)
				if _, ok := shardsMovedTo[subClusterID]; !ok {
					shardsMovedTo[subClusterID] = make(map[uint32]shardMoveInfo)
					shardsMovedTo[subClusterID][subCluster] = shardMoveInfo{movedFrom: subCluster}
				}

				moveInfo := shardsMovedTo[subClusterID][subCluster]
				moveInfo.shardIds = append(moveInfo.shardIds, shardId)
				shardsMovedTo[subCluster][subCluster] = moveInfo
			}
		}
		if h.targetLoad[subClusterID] <= h.currLoad[subClusterID] {
			i++
		}
	}
	return h.subClusterToShardMap, shardsMovedTo, nil
}

func (h *subClusterHelper) moveOneShard(from, to uint32) (bool, uint32, error) {
	if from == to {
		return false, 0, errors.New("source and target subClusters are the same")
	}
	if h.currLoad[from] == h.targetLoad[from] {
		return false, 0, nil
	}
	fromShards := h.subClusterToShardMap[from]
	shardToMove := fromShards[len(fromShards)-1]
	h.subClusterToShardMap[to] = append(h.subClusterToShardMap[to], shardToMove)
	h.subClusterToShardMap[from] = fromShards[:len(fromShards)-1]
	sort.Sort(UInts(h.subClusterToShardMap[to]))
	h.currLoad[from]--
	return true, shardToMove, nil
}

func (h *subClusterHelper) placeShards(ids []uint32) (map[uint32][]uint32, error) {
	clusterHeap := newSubClusterHeap(h.subClusters, h.currLoad, h.targetLoad)
	subClusterToShardMap := make(map[uint32][]uint32)
	sort.Sort(UInts(ids))

	for i := 0; i < len(ids); {
		triedSubClusters := make([]uint32, 0)
		for clusterHeap.Len() > 0 && i < len(ids) {
			subClusterID := clusterHeap.Pop().(uint32)
			triedSubClusters = append(triedSubClusters, subClusterID)
			if h.currLoad[subClusterID] == h.targetLoad[subClusterID] {
				continue
			}
			if _, ok := subClusterToShardMap[subClusterID]; !ok {
				subClusterToShardMap[subClusterID] = make([]uint32, 0)
			}
			shards := subClusterToShardMap[subClusterID]
			shards = append(shards, ids[i])
			subClusterToShardMap[subClusterID] = shards
			h.currLoad[subClusterID]++
			i++
		}
		for _, triedSubCluster := range triedSubClusters {
			clusterHeap.Push(triedSubCluster)
		}
	}

	return subClusterToShardMap, nil
}

func (*subClusterHelper) validateInstancesPerSubCluster(subClusterToInstanceMapping map[uint32][]placement.Instance, rf int) error {
	for subClusterID, instances := range subClusterToInstanceMapping {
		if len(instances)%rf != 0 {
			return errors.New(fmt.Sprintf("sub %d cluster too small, atleast %d instances are required per subcluster", subClusterID, rf))
		}
	}
	return nil
}

type subClusterHeap struct {
	subClusters               []uint32
	subClusterToTotalShardMap map[uint32]int
	targetLoad                map[uint32]int
}

func (h *subClusterHeap) targetLoadForSubCluster(id uint32) int {
	return h.targetLoad[id]
}

func (h *subClusterHeap) Len() int {
	return len(h.subClusters)
}

func (h subClusterHeap) Swap(i, j int) {
	h.subClusters[i], h.subClusters[j] = h.subClusters[j], h.subClusters[i]
}

func (h *subClusterHeap) Push(i interface{}) {
	subCluster := i.(uint32)
	h.subClusters = append(h.subClusters, subCluster)
}

func (h *subClusterHeap) UpdateLoad(subClusterID uint32) {
	h.subClusterToTotalShardMap[subClusterID]++
}

func (h *subClusterHeap) Pop() interface{} {
	old := h.subClusters
	n := len(old)
	subCluster := old[n-1]
	h.subClusters = old[:n-1]
	return subCluster
}

func (h *subClusterHeap) Peek() uint32 {
	if len(h.subClusters) == 0 {
		return 0 // Return a default value if the queue is empty
	}
	return h.subClusters[0]
}

func (h *subClusterHeap) Less(i, j int) bool {
	subClusterI := h.subClusters[i]
	subClusterJ := h.subClusters[j]
	leftShardsOnI := h.targetLoadForSubCluster(subClusterI) - h.subClusterToTotalShardMap[subClusterI]
	leftShardsOnJ := h.targetLoadForSubCluster(subClusterJ) - h.subClusterToTotalShardMap[subClusterJ]
	if leftShardsOnI == leftShardsOnJ {
		return subClusterI < subClusterJ
	}

	return leftShardsOnI > leftShardsOnJ
}

func newSubClusterHeap(subClusters []uint32, subClusterToTotalShardMap map[uint32]int, targetLoad map[uint32]int) *subClusterHeap {
	subClustersCopy := make([]uint32, 0)
	for _, subClusterID := range subClusters {
		subClustersCopy = append(subClustersCopy, subClusterID)
	}
	h := &subClusterHeap{
		subClusters:               subClustersCopy,
		subClusterToTotalShardMap: subClusterToTotalShardMap,
		targetLoad:                targetLoad,
	}
	heap.Init(h)
	return h
}
