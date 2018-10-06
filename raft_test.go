package raft

import (
	"testing"
	"github.com/dzdx/raft/transport"
	"github.com/dzdx/raft/store"
	"time"
	"github.com/stretchr/testify/assert"
	"context"
	"strconv"
	"github.com/dzdx/raft/util/wait"
)

func newTestCluster(servers []string) (*transport.InmemNetwork, map[string]*RaftNode) {
	network := transport.NewInmemNetwork(servers)
	nodes := make(map[string]*RaftNode, len(servers))
	for _, ID := range servers {
		config := DefaultConfig(servers, ID)
		config.VerboseLog =false
		storage := store.NewInmemStore()
		trans := network.GetTrans(ID)
		node := NewRaftNode(config, storage, trans)
		time.Sleep(100 * time.Millisecond)
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
					f.Respond(f.Data, nil)
				}
			}
		}()
		nodes[ID] = node
	}
	return network, nodes
}

func getLeaderNode(nodes map[string]*RaftNode) *RaftNode {
	for ID, node := range nodes {
		if node.state == Leader {
			return nodes[ID]
		}
	}
	return nil
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

func TestLeaderLeaseAndReElectionLeader(t *testing.T) {
	var inmemServers = []string{
		"1", "2", "3",
	}
	network, nodes := newTestCluster(inmemServers)
	time.Sleep(1 * time.Second)
	oldLeader := getLeaderNode(nodes)

	otherPartition := make([]string, 0)
	for ID := range nodes {
		if ID != oldLeader.localID {
			otherPartition = append(otherPartition, ID)
		}
	}
	network.SetPartition([]string{oldLeader.localID}, otherPartition)
	time.Sleep(1 * time.Second)

	assert.NotEqual(t, oldLeader.state, Leader)

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
	leader := getLeaderNode(nodes)
	for i := 0; i < 1000; i++ {
		source := []byte(strconv.Itoa(i))
		resp, err := leader.Apply(context.Background(), source)
		assert.Equal(t, resp, source)
		assert.Equal(t, err, nil)
	}
}

func TestConcurrentAppendEntries(t *testing.T) {
	var inmemServers = []string{
		"1", "2", "3",
	}
	_, nodes := newTestCluster(inmemServers)
	time.Sleep(1 * time.Second)
	leader := getLeaderNode(nodes)
	waitGroup := wait.Group{}
	for i := 0; i < 1000; i++ {
		source := []byte(strconv.Itoa(i))
		waitGroup.Start(func() {
			resp, err := leader.Apply(context.Background(), source)
			assert.Equal(t, resp, source)
			assert.Equal(t, err, nil)
		})
	}
	waitGroup.Wait()
}

func TestFollowerCommitInShortTime(t *testing.T) {
	var inmemServers = []string{
		"1", "2", "3",
	}
	_, nodes := newTestCluster(inmemServers)
	time.Sleep(1 * time.Second)
	leader := getLeaderNode(nodes)
	for i := 0; i < 100; i++ {
		source := []byte(strconv.Itoa(i))
		resp, err := leader.Apply(context.Background(), source)
		assert.Equal(t, resp, source)
		assert.Equal(t, err, nil)
	}
	time.Sleep(50 * time.Millisecond)
	for _, node := range nodes {
		assert.Equal(t, leader.commitIndex, node.commitIndex)
		assert.Equal(t, leader.lastApplied, node.lastApplied)
	}
}

func TestTriggerSnapshot(t *testing.T) {
	var inmemServers = []string{
		"1", "2", "3",
	}
	_, nodes := newTestCluster(inmemServers)
	time.Sleep(1 * time.Second)
	leader := getLeaderNode(nodes)
	for i := 0; i < 1000; i++ {
		resp, err := leader.Apply(context.Background(), []byte("1"))
		assert.Equal(t, resp, []byte("1"))
		assert.Equal(t, err, nil)
	}
}
