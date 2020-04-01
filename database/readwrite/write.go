package readwrite

import (
	"encoding/json"
	"fmt"
	"github.com/ottermad/distrbuteddatabase/database/nodes"
	"io/ioutil"
	"net/http"
)

const WritePath = "/write_to_partition"
func PartitionWriteHandler(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	dbWrite := &partitionWrite{}
	err = json.Unmarshal(body, dbWrite)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	fmt.Printf("Writing %s: %s to partition %d on node: %s \n\n", dbWrite.Key, dbWrite.Value, dbWrite.Partition, nodes.GetOwnAddress())
}

type partitionWrite struct {
	Partition int `json:"partition"`
	Key string `json:"key"`
	Value string `json:"value"`
}
func (p partitionWrite) getBytes() ([]byte, error) {
	output, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}

	return output, nil
}