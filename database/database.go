package database

import (
	"fmt"
	"github.com/ottermad/distrbuteddatabase/database/nodes"
	"net/http"
)

var ownAddress string = ""

func Init(friendlyName string, port string, nodesFile string) {
	ownAddress = "http://localhost:" + port
	nodes.AddOwnNode(friendlyName, ownAddress)

	if nodesFile != "" {
		nodes.ReadNodesFromFile(nodesFile)
	}

	nodes.StartGossiping()
	http.HandleFunc("/ping", pingHandler)
	http.HandleFunc(nodes.ReceiveGossipPath, nodes.ReceiveGossipHandler)

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