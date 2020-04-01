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

type keyRead struct {
	Key string `json:"key"`
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

const CoordinateReadPath = "/coordinate_read"
func CoordinateRead(w http.ResponseWriter, r *http.Request) {
	// Accept key + value
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	dbRead := &keyRead{}
	err = json.Unmarshal(body, dbRead)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	// Calculate partitions
	partitionsForKey := calculatePartitions(dbRead.Key)
	fmt.Printf("Partitions %v for key %s \n\n", partitionsForKey, dbRead.Key)
	// Look up which partitions belong to which node
	successes := 0
	values := []string{}
	for _, partition := range partitionsForKey {
		// Perform keyWrite
		node := partitions.GetAddressForPartition(partition)

		data, err := partitionRead{
			Partition: partition,
			Key:       dbRead.Key,
		}.getBytes()
		if err != nil {
			fmt.Printf("Error %v \n", err)
			continue
		}

		rsp, err := http.Post(node + ReadPath, "text/json", bytes.NewBuffer(data))
		if err != nil {
			fmt.Printf("Error sending request %v \n", err)
			continue
		}
		defer rsp.Body.Close()
		body, err := ioutil.ReadAll(rsp.Body)
		if err != nil {
			fmt.Printf("Error reading body %v \n", err)
			continue
		}

		if rsp.StatusCode > 300 {
			fmt.Println("Error reading %s", string(body))
			continue
		}



		readReceived := &partitionReadResponse{}
		fmt.Println(string(body))
		err = json.Unmarshal(body, readReceived)
		if err != nil {
			fmt.Printf("Error unmarshaling body %v \n", err)
			continue
		}
		values = append(values, readReceived.Value)

		successes += 1
	}

	// Report success or failure
	if successes >= 2 {
		valuesToCount := map[string]int{}
		for _, val := range values {
			count, _ := valuesToCount[val]
			count++
			valuesToCount[val] = count
		}
		max := 0
		val := ""
		for v, c := range valuesToCount {
			if c > max {
				max = c
				val = v
			}
		}
		fmt.Fprint(w, val)
	} else {
		fmt.Fprint(w, "Failed")
	}
}

func calculatePartitions(key string) []int {
	// TODO: Make sure partitions are different
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


