package partitions

import (
	"fmt"
	"github.com/ottermad/distrbuteddatabase/database/nodes"
	"sync"
)

const numberOfPartitions = 10

type Partition struct {
	Node string `json:"node"`
	Number int `json:"number"`
	ReadyToAcceptWrites bool `json:"ready_to_accept_writes"`
	ReadyToAcceptReads bool `json:"ready_to_accept_reads"`
}

var ownPartitions = map[int]interface{}{}
var partitions  = map[int]*Partition{}
var partitionsMutex = sync.RWMutex{}

func GetOwnPartitions() map[int]Partition {
	partitionsMutex.RLock()
	ownPartitionsMap := map[int]Partition{}
	for partition, _  := range ownPartitions {
		ownPartitionsMap[partition] = *partitions[partition]
	}
	partitionsMutex.RUnlock()
	return ownPartitionsMap
}

func GetNumberOfPartition() int {
	return numberOfPartitions
}

func UpdatePartitions(newPartitionsMap map[int]Partition) {
	partitionsMutex.Lock()

	// For each partition
	for _, partition := range newPartitionsMap {
		partition := partition
		oldPartition, existed := partitions[partition.Number]
		//fmt.Printf("setting %d to %v", partition.Number, partition)
		// Update partition
		partitions[partition.Number] = &partition

		// If already existed update our records
		if existed {
			// If we now own the partition mark as unable to accept to writes and reads and trigger background process
			// to stream data
			if partition.Node == nodes.GetOwnAddress() && oldPartition.Node != nodes.GetOwnAddress() {
				ownPartitions[partition.Number] = nil
				partitions[partition.Number].ReadyToAcceptReads = false
				partitions[partition.Number].ReadyToAcceptWrites = false
				// TODO: Trigger streaming process
			}

			// If we no longer own a node remove it from own Partitions
			if partition.Node != nodes.GetOwnAddress() && oldPartition.Node == nodes.GetOwnAddress() {
				delete(ownPartitions, partition.Number)
			}

		// If it is a new node update maps if we own it
		} else {
			if partition.Node == nodes.GetOwnAddress() {
				ownPartitions[partition.Number] = nil
				partitions[partition.Number].ReadyToAcceptReads = false
				partitions[partition.Number].ReadyToAcceptWrites = false
				// TODO: Should we stream here?
			}
		}

	}

	//fmt.Printf("Set partitions to %v \n\n", partitions)

	partitionsMutex.Unlock()
}

func GetAddressForPartition(partition int) string {
	fmt.Println(partitions[partition])
	partitionsMutex.RLock()
	fmt.Println(partitions[partition])
	address := partitions[partition].Node
	partitionsMutex.RUnlock()
	return address
}