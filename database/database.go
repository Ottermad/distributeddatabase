package database

import (
	"fmt"
	"github.com/ottermad/distrbuteddatabase/database/gossip"
	"github.com/ottermad/distrbuteddatabase/database/nodes"
	"github.com/ottermad/distrbuteddatabase/database/partitions"
	"github.com/ottermad/distrbuteddatabase/database/readwrite"
	"net/http"
)

var ownAddress string = ""

func Init(friendlyName string, port string, nodesFile string, dataDirectory string) {
	ownAddress = "http://localhost:" + port
	nodes.AddOwnNode(friendlyName, ownAddress)
	readwrite.SetDataDirectory(dataDirectory)

	if nodesFile != "" {
		nodes.ReadNodesFromFile(nodesFile)
	}

	gossip.StartGossiping()
	http.HandleFunc("/ping", pingHandler)
	http.HandleFunc(gossip.ReceiveGossipPath, gossip.ReceiveGossipHandler)
	http.HandleFunc(partitions.DistrubutedInitialPartitionsPath, partitions.CreateAndDistributeInitialPartitions)
	http.HandleFunc(partitions.ReceiveInitialPartitionsPath, partitions.ReceiveInitialPartitions)
	http.HandleFunc(readwrite.CoordinateWritePath, readwrite.CoordinateWrite)
	http.HandleFunc(readwrite.WritePath, readwrite.PartitionWriteHandler)
	http.HandleFunc(readwrite.CoordinateReadPath, readwrite.CoordinateRead)
	http.HandleFunc(readwrite.ReadPath, readwrite.PartitionReadHandler)

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