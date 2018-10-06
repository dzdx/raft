package raft

import (
	"testing"
	"github.com/dzdx/raft/transport"
	"github.com/dzdx/raft/store"
	"time"
	"github.com/stretchr/testify/assert"
	"context"
)

func newTestCluster(servers []string) (*transport.Network, map[string]*RaftNode) {
	network := transport.NewNetwork(servers)
	nodes := make(map[string]*RaftNode, len(servers))
	for _, ID := range servers {
		config := DefaultConfig(servers, ID)
		storage := store.NewInmemStore()
		trans := network.GetTrans(ID)
		node := NewRaftNode(config, storage, trans)
		go func() {
			for {
				futures := make([]*DataFuture, 0)
			batchRecv:
				for {
					select {
					case future := <-node.CommittedChan():
						futures = append(futures, future)
					default:
						break batchRecv
					}
				}
				for _, f := range futures {
					f.Respond("success", nil)
				}
			}
		}()
		nodes[ID] = node
	}
	return network, nodes
}

func TestElectionLeader(t *testing.T) {

	var inmemServers = []string{
		"1", "2", "3",
	}
	_, nodes := newTestCluster(inmemServers)
	time.Sleep(1 * time.Second)
	leaderCount := 0
	for _, node := range nodes {
		if node.state == Leader {
			leaderCount++
		}
	}
	assert.Equal(t, leaderCount, 1)
}

func TestElectionNoLeader(t *testing.T) {
	var inmemServers = []string{
		"1", "2", "3",
	}
	network, nodes := newTestCluster(inmemServers)
	network.SetPartition([]string{"1"}, []string{"2"}, []string{"3"})
	time.Sleep(1 * time.Second)
	leaderCount := 0
	for _, node := range nodes {
		if node.state == Leader {
			leaderCount++
		}
	}
	assert.Equal(t, leaderCount, 0)
}

func TestReElectionLeader(t *testing.T) {
	var inmemServers = []string{
		"1", "2", "3",
	}
	network, nodes := newTestCluster(inmemServers)
	time.Sleep(1 * time.Second)
	var currentLeader string
	for ID, node := range nodes {
		if node.state == Leader {
			currentLeader = ID
			break
		}
	}
	otherPartition := make([]string, 0)
	for ID, _ := range nodes {
		if ID != currentLeader {
			otherPartition = append(otherPartition, ID)
		}
	}
	network.SetPartition([]string{currentLeader}, otherPartition)
	time.Sleep(1 * time.Second)
	leaderCount := 0
	for _, ID := range otherPartition {
		if nodes[ID].state == Leader {
			leaderCount++
		}
	}
	assert.Equal(t, leaderCount, 1)
}

func TestAppendEntries(t *testing.T) {
	var inmemServers = []string{
		"1", "2", "3",
	}
	_, nodes := newTestCluster(inmemServers)
	time.Sleep(1 * time.Second)
	var leader *RaftNode
	for ID, node := range nodes {
		if node.state == Leader {
			leader = nodes[ID]
			break
		}
	}
	for i := 0; i < 100; i++ {
		resp, _ := leader.Apply(context.Background(), []byte("hello world"))
		assert.Equal(t, resp, "success")
	}
}
