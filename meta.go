package raft

import (
	"encoding/binary"
	"github.com/dzdx/raft/raftpb"
	"github.com/golang/protobuf/proto"
	"github.com/dzdx/raft/store"
	"log"
	"time"
)

type State int

const (
	KeyLastVoted   = "KeyLastVoted"
	KeyCurrentTerm = "KeyCurrentTerm"
)

const (
	Follower State = iota
	Candidate
	Leader
)

const None string = ""

type raftState struct {
	lastVotedFor  string
	lastVotedTerm uint64
	currentTerm   uint64
	lastLogIndex  uint64
	lastLogTerm   uint64

	lastApplied uint64
	commitIndex uint64
	state       State

	localID string
	servers map[string]struct{}

	leader            string
	lastContactLeader time.Time
}

func (r *RaftNode) getCurrentTerm() uint64 {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.currentTerm
}

func (r *RaftNode) setCurrentTerm(term uint64) {
	if r.currentTerm == term {
		return
	}
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, term)
	r.metaStore.SetKV(KeyCurrentTerm, data)

	r.currentTerm = term
}

func (r *RaftNode) getLastVoted() (votedFor string, votedTerm uint64) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	votedFor = r.lastVotedFor
	votedTerm = r.lastVotedTerm
	return
}

func (r *RaftNode) setLastVoted(votedFor string) {
	if r.lastVotedFor == votedFor && r.lastVotedTerm == r.currentTerm {
		return
	}
	lastVoted := &raftpb.LastVoted{
		VotedFor:  votedFor,
		VotedTerm: r.currentTerm,
	}
	data, _ := proto.Marshal(lastVoted)
	r.metaStore.SetKV(KeyLastVoted, data)

	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.lastVotedFor = votedFor
	r.lastVotedTerm = r.currentTerm
}

func (r *RaftNode) restoreMeta() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	var lastVotedFor string
	var lastVotedTerm uint64

	if data, err := r.metaStore.GetKV(KeyLastVoted); err != nil {
		if _, ok := err.(*store.ErrNotFound); ok {
			lastVotedFor = None
			lastVotedTerm = 0
		} else {
			log.Fatalf("read last voted failed %s", err.Error())
		}
	} else {
		var lastVoted *raftpb.LastVoted
		proto.Unmarshal(data, lastVoted)
		lastVotedFor = lastVoted.VotedFor
		lastVotedTerm = lastVoted.VotedTerm
	}
	r.lastVotedFor = lastVotedFor
	r.lastVotedTerm = lastVotedTerm

	var currentTerm uint64
	if data, err := r.metaStore.GetKV(KeyCurrentTerm); err != nil {
		if _, ok := err.(*store.ErrNotFound); ok {
			currentTerm = 0
		} else {
			log.Fatalf("read current term failed %s", err.Error())
		}
	} else {
		currentTerm = binary.BigEndian.Uint64(data)
	}
	r.currentTerm = currentTerm
}

func (r *RaftNode) getState() State {
	return r.state
}

func (r *RaftNode) setState(state State) {
	if r.state != state {
		r.state = state
	}
}

func (r *RaftNode) setLastContactLeader(leaderID string) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.lastContactLeader = time.Now()
	r.leader = leaderID
}

func (r *RaftNode) getLastContactLeader() (leaderID string, lastContact time.Time) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	leaderID = r.leader
	lastContact = r.lastContactLeader
	return
}

func (r *RaftNode) setLastLog(term, index uint64) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.lastLogTerm = term
	r.lastLogIndex = index
}

func (r *RaftNode) getLastLog() (term, index uint64) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	term = r.lastLogTerm
	index = r.lastLogIndex
	return
}
