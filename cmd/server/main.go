package main

import (
	"log"
	"proglog/internal/server"
)

func main() {
	srv := server.NetHTTPServer(":8080")
	log.Fatal(srv.ListenAndServe())
}
