package main

import "github.com/dzdx/raft/raftexample/kvhttp"

func main() {
	kvhttp.NewNode(":8080")
}
