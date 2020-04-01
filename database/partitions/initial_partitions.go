package partitions

import (
	"bytes"
	"fmt"
	"encoding/json"
	"github.com/ottermad/distrbuteddatabase/database/nodes"
	"io/ioutil"
	"net/http"
)

var DistrubutedInitialPartitionsPath = "/distribute_initial_partitions"

func CreateAndDistributeInitialPartitions(w http.ResponseWriter, r *http.Request) {
	// Once cluster is started assign each partition to a node. Send to each node which Partitions it is in charge off.
	nodes := nodes.GetNodes()

	// Get nodes to paritions

	// For each node send it partitions

	keys := []string{}
	for k, _ := range nodes {
		keys = append(keys, k)
	}

	_, nodesToPartitions := MapPartitionsToNodes(keys)

	fmt.Println(nodesToPartitions)
	for node, partitions := range nodesToPartitions {
		address := nodes[node]
		jsonBytes, err := receivedInitialPartitions{
			Partitions: partitions,
		}.getBytes()
		if err != nil {
			fmt.Println(err)
			return
		}
		rsp, err := http.Post(address + ReceiveInitialPartitionsPath, "text/json",  bytes.NewBuffer(jsonBytes))
		defer rsp.Body.Close()
		if err != nil {
			fmt.Println(err)
			return
		}
	}


}

var ReceiveInitialPartitionsPath = "/receive_initial_partitions"

type receivedInitialPartitions struct {
	Partitions []int `json:"Partitions"`
}

func (r receivedInitialPartitions) getBytes() ([]byte, error) {
	output, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}

	return output, nil
}

func ReceiveInitialPartitions(w http.ResponseWriter, r *http.Request) {
	// This will declare which Partitions a node is responsible for on initial start
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	initialPartitions := &receivedInitialPartitions{}
	err = json.Unmarshal(body, initialPartitions)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	partitions := map[int]Partition{}
	for _, num := range initialPartitions.Partitions {
		partitions[num] = Partition{
			Node:                nodes.GetOwnAddress(),
			Number:              num,
			ReadyToAcceptWrites: true,
			ReadyToAcceptReads:  true,
		}
	}



	UpdatePartitions(partitions)
	return
}



func MapPartitionsToNodes(nodes []string) (map[int]Partition, map[string][]int) {
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

	return partitionToNode, nodesToPartition
}

