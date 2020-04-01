package gossip

import (
	"encoding/json"
	"github.com/ottermad/distrbuteddatabase/database/nodes"
	"github.com/ottermad/distrbuteddatabase/database/partitions"
	"io/ioutil"
	"net/http"
)

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

	//fmt.Printf("Received gossp %v \n\n", gossipReceived)
	// Should this acquire the lock for the whole time?
	for friendlyName, address := range gossipReceived.Nodes {
		nodes.AddNode(friendlyName,  address)
	}


	partitions.UpdatePartitions(gossipReceived.PartitionsToNodes)
	return
}

