package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/mux"
)

func main() {
	if len(os.Args) != 2 {
		panic(errors.New("need only one arguement i.e. the port number"))
	}

	m := mux.NewRouter()
	m.Handle("/hello", http.HandlerFunc(hello))
	srv := &http.Server{
		Handler: m,
	}

	nl, err := net.Listen("tcp", fmt.Sprintf(":%s", os.Args[1]))
	if err != nil {
		panic(err)
	}

	go func() {
		if err := srv.Serve(nl); err != http.ErrServerClosed {
			panic(err)
		}
	}()

	time.Sleep(1 * time.Second)
	fmt.Println("Server listening on", nl.Addr())
	for {

	}
}

type Hello struct {
	Age  int
	Name string
}

func hello(w http.ResponseWriter, req *http.Request) {
	rsp := &Hello{
		Age:  25,
		Name: "BajtosTheGreat",
	}

	bz, err := json.Marshal(&rsp)
	if err != nil {
		panic(err)
	}
	w.WriteHeader(200)
	w.Write(bz)
}
