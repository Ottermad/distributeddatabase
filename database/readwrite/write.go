package readwrite

import (
	"encoding/json"
	"fmt"
	"github.com/ottermad/distrbuteddatabase/database/nodes"
	"github.com/ottermad/distrbuteddatabase/database/partitions"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"sync"
)

var dataDirectory = ""

func SetDataDirectory(data string) {
	dataDirectory = data
}

var fileMutex = sync.RWMutex{}

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

	// Verify we own the partition
	if partitions.GetAddressForPartition(dbWrite.Partition) != nodes.GetOwnAddress() {
		http.Error(w, "Do not own partition", 500)
	}

	fileMutex.Lock()
	defer fileMutex.Unlock()

	// Open file if not exists
	filename := dataDirectory + "/" + strconv.Itoa(dbWrite.Partition) + ".data"
	contents, err := ioutil.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			contents = []byte{}
		} else {
			http.Error(w, err.Error(), 500)
			return
		}
	}
	newEntry := fmt.Sprintf("%s:%s\n", dbWrite.Key, dbWrite.Value)
	newContents := append(contents, []byte(newEntry)...)
	err = ioutil.WriteFile(filename, newContents, 0644)
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