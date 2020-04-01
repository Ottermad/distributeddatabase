package gossip

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/ottermad/distrbuteddatabase/database/nodes"
	"github.com/ottermad/distrbuteddatabase/database/partitions"
	"net/http"
	"time"
)

var done = make(chan bool)

func StartGossiping() {
	fmt.Println("Starting gossiping")
	ticker := time.NewTicker(time.Second *2)

	go func() {
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				for _, address := range nodes.GetNodes() {
					// Stop sending gossip to self as it causes locking recursion
					if address != nodes.GetOwnAddress() {
						sendGossipToNode(address)
					}
				}
			}
		}
	}()

}

func sendGossipToNode(address string) {
	gossip := calculateGossip()
	//fmt.Println(gossip)
	jsonBytes, err := gossip.getBytes()
	if err != nil {
		fmt.Println(err)
		return
	}
	rsp, err := http.Post(address +ReceiveGossipPath, "text/json", bytes.NewBuffer(jsonBytes))
	defer rsp.Body.Close()
	if err != nil {
		fmt.Println(err)
		return
	}

}

func calculateGossip() gossip {
	g := gossip{
		Nodes:             nodes.GetNodes(),
		PartitionsToNodes: partitions.GetOwnPartitions(),
	}
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


