package database

import (
	"fmt"
	"net/http"
)

var ownAddress string = ""

func Init(port string) {
	ownAddress = "localhost:" + port

	http.HandleFunc("/ping", pingHandler)
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