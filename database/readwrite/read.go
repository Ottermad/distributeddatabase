package readwrite

import (
	"encoding/json"
	"fmt"
	"github.com/ottermad/distrbuteddatabase/database/nodes"
	"github.com/ottermad/distrbuteddatabase/database/partitions"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
)

type partitionRead struct {
	Partition int `json:"partition"`
	Key string `json:"key"`
}
func (p partitionRead) getBytes() ([]byte, error) {
	output, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}

	return output, nil
}

type partitionReadResponse struct {
	Value string `json:"value"`
}
func (p partitionReadResponse) getBytes() ([]byte, error) {
	output, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}

	return output, nil
}
const ReadPath = "/partition_read"
func PartitionReadHandler(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	dbWrite := &partitionRead{}
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

			http.Error(w, err.Error(), 500)
			return

	}
	stringContents := string(contents)
	rows := strings.Split(stringContents, "\n")
	fmt.Println(rows)
	for i := len(rows) - 1; i >= 0; i-- {
		row := strings.TrimSpace(rows[i])
		if row == "" {
			continue
		}
		data := strings.Split(row, ":")
		fmt.Printf("data %v", data)
		if len(data) != 2 {
			http.Error(w, "Malformed Row", 500)
			return
		}

		if dbWrite.Key == data[0] {
			response := partitionReadResponse{Value: data[1]}
			jsonData, err := json.Marshal(response)
			if err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			fmt.Fprint(w, string(jsonData))
			return
		}

	}

	http.Error(w, "Record not found", 404)

}