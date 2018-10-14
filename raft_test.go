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
	"io"
	"github.com/dzdx/raft/snapshot"
	"bytes"
	"io/ioutil"
)

func testcaseCtx() func() {
	return func() {
		os.Stderr.Sync()
		os.Stdout.Sync()
	}
}

type testFSM struct{}

func (f *testFSM) Apply(ctx context.Context, futures []DataFuture) {
	for _, f := range futures {
		select {
		case <-ctx.Done():
			return
		default:
		}
		f.Respond(f.Data, nil)
	}
}

func (f *testFSM) Snapshot(ctx context.Context) (io.ReadCloser, error) {
	return ioutil.NopCloser(bytes.NewBuffer([]byte("snapshot"))), nil
}

func (f *testFSM) Restore(ctx context.Context, reader io.ReadCloser) error {
	return nil
}

func newClusterManager(servers []string) *clusterManager {
	m := &clusterManager{
		network: transport.NewInmemNetwork(),
		nodes:   make(map[string]*RaftNode),
	}
	for _, server := range servers {
		m.newRaftNode(servers, server)
		time.Sleep(50 * time.Millisecond)
	}
	return m
}

type clusterManager struct {
	nodes   map[string]*RaftNode
	network *transport.InmemNetwork
}

func (m *clusterManager) newRaftNode(servers []string, localID string) *RaftNode {
	config := DefaultConfig(servers, localID)
	//config.VerboseLog = true
	config.MaxReplicationBackoffTimeout = 100 * time.Millisecond
	storage := store.NewInmemStore()
	trans := m.network.Join(localID)
	fsm := &testFSM{}
	snapshotStore := snapshot.NewInmemSnapShotStore()
	node := NewRaftNode(config, storage, trans, fsm, snapshotStore)
	m.nodes[localID] = node
	return node
}
func (m *clusterManager) shutdown() {
	wg := wait.Group{}
	for _, node := range m.nodes {
		wg.Start(node.Shutdown)
	}
	wg.Wait()
}
func (m *clusterManager) getLeader() *RaftNode {
	for _, n := range m.nodes {
		if n.state == Leader {
			return n
		}
	}
	return nil
}
func (m *clusterManager) apply(ctx context.Context, data []byte) (interface{}, error) {
	leader := m.getLeader()
	if leader == nil {
		return nil, ErrNoLeader
	}
	return leader.Apply(ctx, data)
}

func TestElectionLeader(t *testing.T) {
	defer testcaseCtx()()

	manager := newClusterManager([]string{"1", "2", "3"})
	time.Sleep(1 * time.Second)
	leaderCount := 0
	for _, node := range manager.nodes {
		if node.state == Leader {
			leaderCount++
		}
	}
	assert.Equal(t, leaderCount, 1)
	manager.shutdown()
}

func TestElectionNoLeader(t *testing.T) {
	defer testcaseCtx()()

	manager := newClusterManager([]string{"1", "2", "3"})
	manager.network.SetPartition([]string{"1"}, []string{"2"}, []string{"3"})
	time.Sleep(1 * time.Second)
	leaderCount := 0
	for _, node := range manager.nodes {
		if node.state == Leader {
			leaderCount++
		}
	}
	assert.Equal(t, leaderCount, 0)
	manager.shutdown()
}

func TestLeaderLeaseAndReElectionLeader(t *testing.T) {

	defer testcaseCtx()()

	manager := newClusterManager([]string{"1", "2", "3"})
	time.Sleep(1 * time.Second)
	oldLeader := manager.getLeader()

	otherPartition := make([]string, 0)
	for ID := range manager.nodes {
		if ID != oldLeader.localID {
			otherPartition = append(otherPartition, ID)
		}
	}
	manager.network.SetPartition([]string{oldLeader.localID}, otherPartition)
	time.Sleep(1 * time.Second)

	assert.NotEqual(t, oldLeader.state, Leader)

	leaderCount := 0
	for _, ID := range otherPartition {
		if manager.nodes[ID].state == Leader {
			leaderCount++
		}
	}
	assert.Equal(t, leaderCount, 1)
	manager.shutdown()
}

func TestAppendEntries(t *testing.T) {
	defer testcaseCtx()()
	manager := newClusterManager([]string{"1", "2", "3"})
	time.Sleep(1 * time.Second)
	for i := 0; i < 1000; i++ {
		source := []byte(strconv.Itoa(i))
		resp, err := manager.apply(context.Background(), source)
		assert.Equal(t, resp, source)
		assert.Equal(t, err, nil)
	}
	manager.shutdown()
}

func TestNetworkPartitionAppendEntries(t *testing.T) {
	defer testcaseCtx()()

	manager := newClusterManager([]string{"1", "2", "3"})
	manager.network.SetPartition([]string{"1"}, []string{"2", "3"})
	time.Sleep(1 * time.Second)
	for i := 0; i < 1000; i++ {
		source := []byte(strconv.Itoa(i))
		resp, err := manager.apply(context.Background(), source)
		assert.Equal(t, resp, source)
		assert.Equal(t, err, nil)
	}
	manager.shutdown()
}

func TestConcurrentAppendEntries(t *testing.T) {
	defer testcaseCtx()()
	manager := newClusterManager([]string{"1", "2", "3"})
	time.Sleep(1 * time.Second)
	leader := manager.getLeader()
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
	manager.shutdown()
}

func TestFollowerCommitInShortTime(t *testing.T) {
	defer testcaseCtx()()
	manager := newClusterManager([]string{"1", "2", "3"})
	time.Sleep(1 * time.Second)
	leader := manager.getLeader()
	for i := 0; i < 100; i++ {
		source := []byte(strconv.Itoa(i))
		resp, err := leader.Apply(context.Background(), source)
		assert.Equal(t, resp, source)
		assert.Equal(t, err, nil)
	}
	time.Sleep(200 * time.Millisecond)
	for _, node := range manager.nodes {
		assert.Equal(t, leader.commitIndex, node.commitIndex)
		assert.Equal(t, leader.lastApplied, node.lastApplied)
	}
	manager.shutdown()
}

func TestTriggerSnapshot(t *testing.T) {
	defer testcaseCtx()()
	manager := newClusterManager([]string{"1", "2", "3"})
	time.Sleep(1 * time.Second)
	leader := manager.getLeader()
	for i := 0; i < 1000; i++ {
		leader.Apply(context.Background(), []byte("1"))
	}

	var index uint64
	index, _ = leader.entryStore.LastIndex()
	assert.Equal(t, index, uint64(1001))
	leader.Snapshot()
	time.Sleep(50 * time.Millisecond)
	assert.NotEqual(t, leader.snapshoter.Last(), nil)
	meta := leader.snapshoter.Last()
	snap, _ := leader.snapshoter.Open(meta.ID)
	content, _ := ioutil.ReadAll(snap.Content())
	assert.Equal(t, content, []byte("snapshot"))

	index, _ = leader.entryStore.FirstIndex()
	assert.Equal(t, index, uint64(0))
	index, _ = leader.entryStore.LastIndex()
	assert.Equal(t, index, uint64(0))
	leader.Apply(context.Background(), []byte("1"))
	index, _ = leader.entryStore.FirstIndex()
	assert.Equal(t, index, uint64(1002))
	index, _ = leader.entryStore.LastIndex()
	assert.Equal(t, index, uint64(1002))
	manager.shutdown()
}
func TestSendInstallSnapshotToBackwardFollower(t *testing.T) {
	manager := newClusterManager([]string{"1", "2", "3"})
	manager.network.SetPartition([]string{"1", "2"}, []string{"3"})
	n3 := manager.nodes["3"]
	time.Sleep(1 * time.Second)
	leader := manager.getLeader()
	for i := 0; i < 100; i++ {
		leader.Apply(context.Background(), []byte("1"))
	}
	index, _ := n3.entryStore.LastIndex()
	assert.Equal(t, index, uint64(0))
	leader.Snapshot()

	n3.resetElectionTimer()
	// to avoid high term n3 cause leader step down
	n3.currentTerm = leader.currentTerm

	manager.network.SetPartition([]string{"1", "2", "3"})
	time.Sleep(200 * time.Millisecond)

	assert.Equal(t, n3.lastIndex(), uint64(101))
	assert.Equal(t, n3.lastApplied, uint64(101))
	assert.NotEqual(t, n3.snapshoter.Last(), nil)
	meta := n3.snapshoter.Last()
	snap, _ := n3.snapshoter.Open(meta.ID)
	content, _ := ioutil.ReadAll(snap.Content())
	assert.Equal(t, content, []byte("snapshot"))

	manager.apply(context.Background(), []byte("2"))
	time.Sleep(200 * time.Millisecond)
	for _, n := range manager.nodes {
		assert.Equal(t, n.lastIndex(), uint64(102))
	}
	manager.shutdown()
}

func TestFollowerRejectRequestVoteWhenHasLeader(t *testing.T) {
	// let three raft nodes elected leader
	// period apply log to the leader
	// isolation a node's network
	// manual increase the node's term (if enable prevote)
	// restore the node's network
	// apply log will not failed
}

func TestRetryReplicateAfterReplicateErrorAndRespToClient(t *testing.T) {
	// isolation a node's network
	// manual period reset the node electionTimer (to avoid become candidate)
	// applied a log to leader
	// restore  the node's network and sleep for a moment
	// check node lastLogIndex
}

func TestMostLogsNodeBecomeLeader(t *testing.T) {
	// manual store some logs to three nodes entryStore
	// let they elect leader
	// check who is the leader
}

func TestAutoRemoveConflictLogInFollower(t *testing.T) {
	// let three raft nodes elected leader
	// isolation the leader and apply a log to it
	// waiting for the other two nodes elected leader
	// check the last leader's log will be removed:W
}
