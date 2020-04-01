package main

import (
	"github.com/ottermad/distrbuteddatabase/database"
	"os"
)

func main() {
	argsWithoutProg := os.Args[1:]
	friendlyName := argsWithoutProg[0]
	port := argsWithoutProg[1]
	dataDirectory := argsWithoutProg[2]
	nodesFile := ""
	if len(argsWithoutProg) > 3 {
		nodesFile = argsWithoutProg[3]
	}
	database.Init(friendlyName, port, nodesFile, dataDirectory)
}
