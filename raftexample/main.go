package main

import "github.com/dzdx/raft/raftexample/kvhttp"

func main() {
	raftAddrs := map[string]string{
		"1": "127.0.0.1:9081",
		"2": "127.0.0.1:9082",
		"3": "127.0.0.1:9083",
	}
	webAddrs := map[string]string{
		"1": ":8081",
		"2": ":8082",
		"3": ":8083",
	}
	for ID := range raftAddrs {
		config := kvhttp.NodeConfig{
			Raftaddrs: raftAddrs,
			Webaddrs:  webAddrs,
			LocalID:   ID,
		}
		node := kvhttp.NewNode(config)
		go node.Run()
	}
	quit := make(chan struct{})
	<-quit
}
