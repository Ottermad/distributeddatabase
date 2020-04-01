package nodes

import (
	"encoding/json"
	"io/ioutil"
	"sync"
)

var nodes map[string]string = map[string]string{}
var ownAddress = ""

var mutex = sync.RWMutex{}

func GetOwnAddress() string {
	return ownAddress
}

func AddOwnNode(friendlyName string, address string) {
	AddNode(friendlyName, address)
	ownAddress = address
}

func AddNode(friendlyName string, address string) {
	mutex.Lock()
	nodes[friendlyName] = address
	mutex.Unlock()
}

func GetNodes() map[string]string {
	mutex.RLock()
	copyOfNodeMap := map[string]string{}
	for k,v := range nodes {
		copyOfNodeMap[k] = v
	}
	mutex.RUnlock()
	return copyOfNodeMap
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

