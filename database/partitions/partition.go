package partitions

import "sync"

const numberOfPartitions = 1000

type Partition struct {
	Node string `json:"node"`
	Number int `json:"number"`
	ReadyToAcceptWrites bool `json:"ready_to_accept_writes"`
	ReadyToAcceptReads bool `json:"ready_to_accept_reads"`
}

var partitions []Partition = []Partition{}
var partitionsMutex = sync.RWMutex{}

func GetPartitions() []Partition {
	partitionsMutex.RLock()
	newList := make([]Partition, len(partitions))
	copy(newList, partitions)
	partitionsMutex.RUnlock()
	return newList
}

func MapPartitionsToNodes(nodes []string) map[int]Partition {
	numberOfNodes := len(nodes)
	partitionsPerNode := numberOfPartitions / numberOfNodes // Rounds down

	nodesToPartition := map[string][]int{}
	for _, node := range nodes {
		nodesToPartition[node] = []int{}
	}

	partitionToNode := map[int]Partition{}

	for partition := 1; partition <= numberOfPartitions; partition++ {
		// Cycle through nodes
		// If node not full add partition
		filled := false
		for _, node := range nodes {
			if (len(nodesToPartition[node])) >= partitionsPerNode {
				continue
			}

			nodesToPartition[node] = append(nodesToPartition[node], partition)
			partitionToNode[partition] = Partition{
				Node:                node,
				Number:              partition,
			}
			filled = true
			break
		}

		if !filled {
			// If all nodes full then allow adding one extra partition
			for _, node := range nodes {
				if (len(nodesToPartition[node])) >= partitionsPerNode+1 {
					continue
				}

				nodesToPartition[node] = append(nodesToPartition[node], partition)
				partitionToNode[partition] = Partition{
					Node:                node,
					Number:              partition,
				}
				break
			}
		}
	}

	return partitionToNode
}
