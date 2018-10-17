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
	"github.com/dzdx/raft/raftpb"
	"github.com/dzdx/raft/util"
	"github.com/golang/protobuf/proto"
	"encoding/binary"
)

func testcaseCtx() func() {
	return func() {
		os.Stderr.Sync()
		os.Stdout.Sync()
	}
}

func newTestFSM() *testFSM {
	return &testFSM{
		data: make([]byte, 0),
	}
}

type testFSM struct {
	data []byte
}

func (fsm *testFSM) Apply(ctx context.Context, futures []DataFuture) {
	for _, f := range futures {
		select {
		case <-ctx.Done():
			return
		default:
		}
		fsm.data = f.Data
		f.Respond(f.Data, nil)
	}
}

func (f *testFSM) Snapshot(ctx context.Context) (io.ReadCloser, error) {
	return ioutil.NopCloser(bytes.NewBuffer(f.data)), nil
}

func (f *testFSM) Restore(ctx context.Context, reader io.ReadCloser) error {
	data, _ := ioutil.ReadAll(reader)
	f.data = data
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
	config.VerboseLog = true
	config.MaxReplicationBackoffTimeout = 100 * time.Millisecond
	storage := store.NewInmemStore()
	trans := m.network.Join(localID)
	fsm := newTestFSM()
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
	assert.Equal(t, 1, leaderCount)
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
	assert.Equal(t, 0, leaderCount)
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
	assert.Equal(t, 1, leaderCount)
	manager.shutdown()
}

func TestAppendEntries(t *testing.T) {
	defer testcaseCtx()()
	manager := newClusterManager([]string{"1", "2", "3"})
	time.Sleep(1 * time.Second)
	for i := 0; i < 1000; i++ {
		source := []byte(strconv.Itoa(i))
		resp, err := manager.apply(context.Background(), source)
		assert.Equal(t, source, resp)
		assert.Equal(t, nil, err)
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
		assert.Equal(t, source, resp)
		assert.Equal(t, nil, err)
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
			assert.Equal(t, source, resp)
			assert.Equal(t, nil, err)
		})
	}
	waitGroup.Wait()
	assert.Equal(t, 0, len(leader.leaderState.inflightingFutures))
	assert.Equal(t, uint64(1001), leader.lastIndex())
	for i := uint64(1); i <= leader.lastIndex(); i++ {
		entry, err := leader.entryStore.GetEntry(i)
		assert.Equal(t, nil, err)
		assert.Equal(t, i, entry.Index)
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
		assert.Equal(t, source, resp)
		assert.Equal(t, nil, err)
	}
	time.Sleep(500 * time.Millisecond)
	for _, node := range manager.nodes {
		assert.Equal(t, node.commitIndex, leader.commitIndex)
		assert.Equal(t, node.lastApplied, leader.lastApplied)
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
	assert.Equal(t, uint64(1001), index)
	leader.Snapshot()
	time.Sleep(50 * time.Millisecond)
	assert.NotEqual(t, leader.snapshoter.Last(), nil)
	meta := leader.snapshoter.Last()
	snap, _ := leader.snapshoter.Open(meta.ID)

	content, _ := ioutil.ReadAll(snap.Content())

	confData, _ := proto.Marshal(leader.configurations.last)
	var confLen = make([]byte, 4)
	binary.BigEndian.PutUint32(confLen, uint32(len(confData)))
	var buf bytes.Buffer
	buf.Write(confLen)
	buf.Write(confData)
	buf.WriteString("1")
	assert.Equal(t, buf.Bytes(), content)

	index, _ = leader.entryStore.FirstIndex()
	assert.Equal(t, uint64(0), index)
	index, _ = leader.entryStore.LastIndex()
	assert.Equal(t, uint64(0), index)
	leader.Apply(context.Background(), []byte("1"))
	index, _ = leader.entryStore.FirstIndex()
	assert.Equal(t, uint64(1002), index)
	index, _ = leader.entryStore.LastIndex()
	assert.Equal(t, uint64(1002), index)
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
	assert.Equal(t, uint64(0), index)
	leader.Snapshot()

	n3.resetElectionTimer()
	// to avoid high term n3 cause leader step down
	n3.currentTerm = leader.currentTerm

	manager.network.SetPartition([]string{"1", "2", "3"})
	time.Sleep(200 * time.Millisecond)

	assert.Equal(t, uint64(101), n3.lastIndex())
	assert.Equal(t, uint64(101), n3.lastApplied)
	assert.NotEqual(t, n3.snapshoter.Last(), nil)
	fsm := n3.fsm.(*testFSM)
	assert.Equal(t, []byte("1"), fsm.data)

	manager.apply(context.Background(), []byte("2"))
	time.Sleep(200 * time.Millisecond)
	for _, n := range manager.nodes {
		assert.Equal(t, uint64(102), n.lastIndex(), n.localID)
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

func TestMergeConfiguration(t *testing.T) {
	oldServerIDs := []string{"1", "2", "3"}
	oldConf := &raftpb.Configuration{
		Nodes: make([]*raftpb.Node, 0, len(oldServerIDs)),
	}
	for _, s := range oldServerIDs {
		oldConf.Nodes = append(oldConf.Nodes, &raftpb.Node{
			ServerID: s,
		})
	}
	var newConf *raftpb.Configuration
	var err error
	newConf, _ = mergeConfiguration(oldConf, &raftpb.ConfChange{
		Type:     raftpb.ConfChange_AddNode,
		ServerID: "4",
	})
	assert.Equal(t, 4, len(util.NodesToIDs(newConf.Nodes)))
	_, err = mergeConfiguration(oldConf, &raftpb.ConfChange{
		Type:     raftpb.ConfChange_AddNode,
		ServerID: "3",
	})
	assert.NotEqual(t, err, nil)

	for _, s := range oldServerIDs {
		newConf, _ = mergeConfiguration(oldConf, &raftpb.ConfChange{
			Type:     raftpb.ConfChange_RemoveNode,
			ServerID: s,
		})
		assert.Equal(t, 2, len(util.NodesToIDs(newConf.Nodes)))
		assert.NotContains(t, util.NodesToIDs(newConf.Nodes), s)
	}
	_, err = mergeConfiguration(oldConf, &raftpb.ConfChange{
		Type:     raftpb.ConfChange_RemoveNode,
		ServerID: "4",
	})
	assert.NotEqual(t, err, nil)
}

func TestAddVoter(t *testing.T) {
	manager := newClusterManager([]string{"1", "2", "3"})
	time.Sleep(500 * time.Millisecond)
	leader := manager.getLeader()
	for i := 0; i < 100; i++ {
		leader.Apply(context.Background(), []byte("1"))
	}
	leader.AddVoter(context.Background(), "4")
	manager.newRaftNode([]string{"1", "2", "3", "4"}, "4")
	n4 := manager.nodes["4"]
	time.Sleep(1 * time.Second)
	assert.Equal(t, uint64(102), n4.lastLogIndex)
}
