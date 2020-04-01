package nodes

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/ottermad/distrbuteddatabase/database/partitions"
	"io/ioutil"
	"net/http"
	"sync"
	"time"
)

var nodes map[string]string = map[string]string{}
var done = make(chan bool)
var ownAddress = ""

var mutex = sync.RWMutex{}

func AddOwnNode(friendlyName string, address string) {
	AddNode(friendlyName, address)
	ownAddress = address
}

func AddNode(friendlyName string, address string) {
	mutex.Lock()
	nodes[friendlyName] = address
	mutex.Unlock()
}

func StartGossiping() {
	ticker := time.NewTicker(time.Second *5)

	go func() {
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				mutex.RLock()
				for _, address := range nodes {
					// Stop sending gossip to self as it causes locking recursion
					if address != ownAddress {
						sendGossipToNode(address)
					}
				}
				mutex.RUnlock()
			}
		}
	}()

}

func sendGossipToNode(address string) {
	gossip := calculateGossip()
	jsonBytes, err := gossip.getBytes()
	if err != nil {
		fmt.Println(err)
		return
	}
	rsp, err := http.Post(address + ReceiveGossipPath, "text/json", bytes.NewBuffer(jsonBytes))
	defer rsp.Body.Close()
	if err != nil {
		fmt.Println(err)
		return
	}

}

func calculateGossip() gossip {
	g := gossip{
		Nodes:nodes,
	}
	partitionsToAddress := map[int]partitions.Partition{}
	for _, partition := range partitions.GetPartitions() {
		partitionsToAddress[partition.Number] = partition
	}
	g.PartitionsToNodes = partitionsToAddress
	return g
}

type gossip struct {
	Nodes map[string]string `json:"nodes"`
	PartitionsToNodes map[int]partitions.Partition `json:"partitions_to_nodes"`
}

func (g gossip) getBytes() ([]byte, error) {
	output, err := json.Marshal(g)
	if err != nil {
		return nil, err
	}

	return output, nil
}

var ReceiveGossipPath = "/receive_gossip"
func ReceiveGossipHandler(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	gossipReceived := &gossip{}
	err = json.Unmarshal(body, gossipReceived)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	mutex.Lock()
	for friendlyName, address := range gossipReceived.Nodes {
		nodes[friendlyName] = address
	}
	mutex.Unlock()
	return
}


func ReadNodesFromFile(path string) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}

	nodesJSON := &struct {
		Nodes map[string]string `json:"nodes"`
	}{}
	err = json.Unmarshal(data, nodesJSON)
	if err != nil {
		panic(err)
	}

	for friendlyName, address := range nodesJSON.Nodes {
		AddNode(friendlyName, address)
	}
}