package raft

import (
	"sync"
	"github.com/dzdx/raft/util"
	"sort"
	"github.com/dzdx/raft/raftpb"
)

type commitment struct {
	matchIndexes map[string]uint64
	mutex        sync.Mutex
	commitIndex  uint64
	servers      []string
	commitCh     chan struct{}
}

func newcommitment(servers map[string]struct{}) *commitment {
	c := &commitment{
		matchIndexes: make(map[string]uint64, len(servers)),
		commitCh:     make(chan struct{}, 1),
	}
	for s := range servers {
		c.matchIndexes[s] = 0
	}
	return c
}

func (c *commitment) GetMatchIndex(serverID string) uint64 {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.matchIndexes[serverID]
}

func (c *commitment) SetConfiguration(conf *raftpb.Configuration) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	matchIndexes := make(map[string]uint64)
	for _, node := range conf.Nodes {
		if node.Role == raftpb.NodeRole_Voter {
			matchIndexes[node.ServerID] = c.matchIndexes[node.ServerID]
		}
	}
	c.matchIndexes = matchIndexes
}

func (c *commitment) SetMatchIndex(serverID string, matchIndex uint64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if _, ok := c.matchIndexes[serverID]; !ok {
		return
	}
	c.matchIndexes[serverID] = matchIndex
	oldCommitIndex := c.commitIndex
	c.updateCommitIndex()
	if oldCommitIndex < c.commitIndex {
		util.AsyncNotify(c.commitCh)
	}
}

func (c *commitment) updateCommitIndex() {
	indexes := make(int64Slice, 0, len(c.matchIndexes))
	for _, index := range c.matchIndexes {
		indexes = append(indexes, index)
	}
	sort.Sort(indexes)
	c.commitIndex = indexes[(len(indexes)-1)/2]
}

type int64Slice []uint64

func (s int64Slice) Len() int {
	return len(s)
}

func (s int64Slice) Less(i, j int) bool {
	return s[i] < s[j]
}

func (s int64Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
