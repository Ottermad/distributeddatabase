package readwrite

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/ottermad/distrbuteddatabase/database/partitions"
	"io/ioutil"
	"net/http"
	"github.com/spaolacci/murmur3"
)

type keyWrite struct {
	Key string `json:"key"`
	Value string `json:"value"`
}


const CoordinateWritePath = "/coordinate_write"
func CoordinateWrite(w http.ResponseWriter, r *http.Request) {
	// Accept key + value
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	dbWrite := &keyWrite{}
	err = json.Unmarshal(body, dbWrite)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	// Calculate partitions
	partitionsForKey := calculatePartitions(dbWrite.Key)
	fmt.Printf("Partitions %v for key %s \n\n", partitionsForKey, dbWrite.Key)
	// Look up which partitions belong to which node
	successes := 0
	for _, partition := range partitionsForKey {
		// Perform keyWrite
		node := partitions.GetAddressForPartition(partition)

		data, err := partitionWrite{
			Partition: partition,
			Key:       dbWrite.Key,
			Value:     dbWrite.Value,
		}.getBytes()
		if err != nil {
			fmt.Printf("Error %v \n", err)
			continue
		}

		_, err = http.Post(node + WritePath, "text/json", bytes.NewBuffer(data))
		if err != nil {
			fmt.Printf("Error %v \n", err)
			continue
		}


		successes += 1
	}

	// Report success or failure
	if successes >= 2 {
		fmt.Fprint(w, "Sucess")
	} else {
		fmt.Fprint(w, "Failed")
	}
}

func calculatePartitions(key string) []int {
	// Generate Hash
	hash := murmur3.Sum64([]byte(key))
	numberOfPartitions := uint64(partitions.GetNumberOfPartition())
	// Hash mod 1000
	partition1 := hash % numberOfPartitions
	// Hash * 2 mod 1000
	partition2 := (hash * 2) % numberOfPartitions
	// Hash * 3 mod 1000
	partition3 := (hash*3) % numberOfPartitions

	return []int{int(partition1), int(partition2), int(partition3)}
}


