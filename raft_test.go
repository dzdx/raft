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
	"os"
)

func testcaseCtx() func() {
	return func() {
		os.Stderr.Sync()
		os.Stdout.Sync()
	}
}

func newTestCluster(servers []string) (*transport.InmemNetwork, map[string]*RaftNode) {
	network := transport.NewInmemNetwork(servers)
	nodes := make(map[string]*RaftNode, len(servers))
	for _, ID := range servers {
		config := DefaultConfig(servers, ID)
		config.VerboseLog = false
		storage := store.NewInmemStore()
		trans := network.GetTrans(ID)
		node := NewRaftNode(config, storage, trans)
		time.Sleep(100 * time.Millisecond)
		go func() {
			for {
				select {
				case future := <-node.CommittedChan():
					future.Respond(future.Data, nil)
				case <-node.CheckQuit():
					return
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

func shutdownNodes(nodes map[string]*RaftNode) {
	wg := wait.Group{}
	for _, node := range nodes {
		wg.Start(node.Shutdown)
	}
	wg.Wait()
}

func TestElectionLeader(t *testing.T) {
	defer testcaseCtx()()

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
	shutdownNodes(nodes)
}

func TestElectionNoLeader(t *testing.T) {
	defer testcaseCtx()()

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
	shutdownNodes(nodes)
}

func TestLeaderLeaseAndReElectionLeader(t *testing.T) {

	defer testcaseCtx()()

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
	shutdownNodes(nodes)
}

func TestMostLogsNodeBecomeLeader(t *testing.T) {
	// TODO
}

func TestAppendEntries(t *testing.T) {
	defer testcaseCtx()()
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
	shutdownNodes(nodes)
}

func TestNetworkPartitionAppendEntries(t *testing.T) {
	defer testcaseCtx()()

	var inmemServers = []string{
		"1", "2", "3",
	}
	network, nodes := newTestCluster(inmemServers)
	network.SetPartition([]string{"1"}, []string{"2", "3"})
	time.Sleep(1 * time.Second)
	leader := getLeaderNode(nodes)
	for i := 0; i < 1000; i++ {
		source := []byte(strconv.Itoa(i))
		resp, err := leader.Apply(context.Background(), source)
		assert.Equal(t, resp, source)
		assert.Equal(t, err, nil)
	}
	shutdownNodes(nodes)
}

func TestConcurrentAppendEntries(t *testing.T) {
	defer testcaseCtx()()
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
	assert.Equal(t, leader.lastIndex(), uint64(1001))
	for i := uint64(1); i <= leader.lastIndex(); i++ {
		entry, err := leader.entryStore.GetEntry(i)
		assert.Equal(t, err, nil)
		assert.Equal(t, entry.Index, i)
	}
	shutdownNodes(nodes)
}

func TestFollowerCommitInShortTime(t *testing.T) {
	defer testcaseCtx()()
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
	shutdownNodes(nodes)
}

func TestTriggerSnapshot(t *testing.T) {
	defer testcaseCtx()()
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
	shutdownNodes(nodes)
}
