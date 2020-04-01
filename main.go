package main

import (
	"github.com/ottermad/distrbuteddatabase/database"
	"os"
)

func main() {
	argsWithoutProg := os.Args[1:]
	friendlyName := argsWithoutProg[0]
	port := argsWithoutProg[1]
	nodesFile := ""
	if len(argsWithoutProg) > 2 {
		nodesFile = argsWithoutProg[2]
	}
	database.Init(friendlyName, port, nodesFile)
}
