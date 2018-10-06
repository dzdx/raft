package raft

import (
	"sync"
	"github.com/dzdx/raft/util"
	"sort"
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

func (c *commitment) SetMatchIndex(serverID string, matchIndex uint64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

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
