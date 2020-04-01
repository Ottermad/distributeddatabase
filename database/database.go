package database

import (
	"fmt"
	"github.com/ottermad/distrbuteddatabase/database/gossip"
	"github.com/ottermad/distrbuteddatabase/database/nodes"
	"github.com/ottermad/distrbuteddatabase/database/partitions"
	"net/http"
)

var ownAddress string = ""

func Init(friendlyName string, port string, nodesFile string) {
	ownAddress = "http://localhost:" + port
	nodes.AddOwnNode(friendlyName, ownAddress)

	if nodesFile != "" {
		nodes.ReadNodesFromFile(nodesFile)
	}

	gossip.StartGossiping()
	http.HandleFunc("/ping", pingHandler)
	http.HandleFunc(gossip.ReceiveGossipPath, gossip.ReceiveGossipHandler)
	http.HandleFunc(partitions.DistrubutedInitialPartitionsPath, partitions.CreateAndDistributeInitialPartitions)
	http.HandleFunc(partitions.ReceiveInitialPartitionsPath, partitions.ReceiveInitialPartitions)

	err := http.ListenAndServe(":" + port, nil)
	if err != nil {
		panic(err)
	}
}

func pingHandler(w http.ResponseWriter, r *http.Request) {
	_, err := fmt.Fprint(w, "ping")
	if err != nil {
		w.WriteHeader(500)
	}
}