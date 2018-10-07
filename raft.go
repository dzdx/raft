package raft

import (
	"github.com/dzdx/raft/util/wait"
	"time"
	"sync"
	"github.com/dzdx/raft/store"
	"github.com/dzdx/raft/transport"
	"github.com/dzdx/raft/raftpb"
	"github.com/dzdx/raft/util"
	"context"
	"github.com/sirupsen/logrus"
	"errors"
)

var (
	ErrNotLeader = errors.New("not leader")
)

type leaderState struct {
	ctx                context.Context
	cancelFunc         context.CancelFunc
	commitment         *commitment
	followers          map[string]*Progress
	waitGroup          wait.Group
	inflightingFutures map[uint64]*ApplyFuture
}

type RaftConfig struct {
	MaxInflightingEntries int
	MaxBatchAppendEntries int
	ElectionTimeout       time.Duration
	Servers               []string
	LocalID               string
	VerboseLog            bool
}

type RaftNode struct {
	raftState

	config      RaftConfig
	applyCh     chan *ApplyFuture
	committedCh chan *DataFuture

	notifyApplyCh chan struct{}

	waitGroup   wait.Group
	ctx         context.Context
	cancelFunc  context.CancelFunc
	mutex       sync.Mutex
	leaderState *leaderState

	entryStore store.IStore
	metaStore  store.IStore
	transport  transport.ITransport

	logger *logrus.Logger

	electionTimer <-chan time.Time
}

func (r *RaftNode) Shutdown() {
	r.cancelFunc()
	r.transport.Shutdown()
	r.waitGroup.Wait()
}

func (r *RaftNode) resetElectionTimer() {
	r.electionTimer = time.After(util.RandomDuration(r.config.ElectionTimeout))
}

func (r *RaftNode) runFollower() {
	r.logger.Info("become follower")
	r.resetElectionTimer()
	for r.state == Follower {
		select {
		case <-r.electionTimer:
			r.leader = None
			r.setState(Candidate)
			return
		case <-r.ctx.Done():
			return
		case rpc := <-r.transport.RecvRPC():
			r.processRPC(rpc)
		case future := <-r.applyCh:
			future.Respond(nil, ErrNotLeader)
		}
	}
}

func (r *RaftNode) processRPC(rpc *transport.RPC) {
	switch req := rpc.Req.(type) {
	case *raftpb.AppendEntriesReq:
		r.processAppendEntries(rpc, req)
	case *raftpb.RequestVoteReq:
		r.processRequestVote(rpc, req)
	}
}

func (r *RaftNode) processAppendEntries(rpc *transport.RPC, req *raftpb.AppendEntriesReq) {
	resp := &raftpb.AppendEntriesResp{
		Success: false,
	}
	defer func() {
		resp.Term = r.getCurrentTerm()
		resp.LastLogIndex = r.lastLogIndex
		rpc.Respond(resp, nil)
	}()

	if req.Term < r.currentTerm {
		return
	}

	if req.Term > r.currentTerm {
		r.setCurrentTerm(req.Term)
	}
	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex > r.lastLogIndex {
			return
		}
		var prevLog *raftpb.LogEntry
		var err error
		if prevLog, err = r.entryStore.GetEntry(req.PrevLogIndex); err != nil {
			r.logger.Errorf("get entry failed: %s", err.Error())
			return
		}
		if prevLog.Term != req.PrevLogTerm {
			return
		}
	}

	if len(req.Entries) > 0 {
		newStart := 0
		for i, entry := range req.Entries {
			newStart = i
			e, err := r.entryStore.GetEntry(entry.Index)
			if err != nil {
				if _, ok := err.(*store.ErrNotFound); ok {
					break
				} else {
					r.logger.Errorf("get entry failed: %s", err)
					return
				}
			}
			if e.Term != entry.Term {
				break
			}
		}

		var lastLog *raftpb.LogEntry
		// delete conflict log entries
		if err := r.entryStore.DeleteEntries(req.Entries[newStart].Index, r.lastLogIndex); err != nil {
			r.logger.Errorf("delete entries failed: %s", err.Error())
			return
		}
		if newStart-1 > 0 {
			lastLog = req.Entries[newStart-1]
			r.setLastLog(lastLog.Term, lastLog.Index)
		} else {
			r.setLastLog(0, 0)
		}

		newEntries := req.Entries[newStart:]
		if err := r.entryStore.AppendEntries(newEntries); err != nil {
			r.logger.Errorf("append entries failed: %s", err.Error())
			return
		}
		lastLog = newEntries[len(newEntries)-1]
		r.setLastLog(lastLog.Term, lastLog.Index)
	}

	if req.LeaderCommitIndex > 0 {
		r.commitTo(util.MinUint64(req.LeaderCommitIndex, r.lastLogIndex))
	}

	r.setState(Follower)
	r.setLastContactLeader(req.LeaderID)
	r.resetElectionTimer()
	resp.Success = true
}

func (r *RaftNode) processRequestVote(rpc *transport.RPC, req *raftpb.RequestVoteReq) {
	resp := &raftpb.RequestVoteResp{VoteGranted: false}
	defer func() {
		resp.Term = r.getCurrentTerm()
		rpc.Respond(resp, nil)
	}()

	leaderID, lastContactLeader := r.getLastContactLeader()
	if leaderID != None && leaderID != req.CandidateID && time.Now().Sub(lastContactLeader) < r.config.ElectionTimeout {
		// now this node has leader
		return
	}
	if r.lastVotedTerm == req.Term && r.lastVotedFor != None && r.lastVotedFor != req.CandidateID {
		// has voted to other node in this term
		return
	}
	if req.Term < r.currentTerm {
		return
	}
	lastLogTerm, lastLogIndex := r.getLastLog()
	if req.LastLogIndex < lastLogIndex || req.LastLogTerm < lastLogTerm {
		// candidate logs not complete
		return
	}

	resp.VoteGranted = true
	if req.Term > r.currentTerm {
		r.setCurrentTerm(req.Term)
		r.setState(Follower)
	}
	r.setLastVoted(req.CandidateID)
	return
}

func (r *RaftNode) startElection() <-chan *raftpb.RequestVoteResp {
	r.setCurrentTerm(r.getCurrentTerm() + 1)
	lastLogTerm, lastLogIndex := r.getLastLog()
	req := &raftpb.RequestVoteReq{
		Term:         r.getCurrentTerm(),
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
		CandidateID:  r.localID,
	}
	respChan := make(chan *raftpb.RequestVoteResp, len(r.servers))
	for s := range r.servers {
		if s == r.localID {
			r.setLastVoted(r.localID)
			respChan <- &raftpb.RequestVoteResp{
				Term:        r.getCurrentTerm(),
				VoteGranted: true,
			}
		} else {
			serverID := s
			r.waitGroup.Start(func() {
				ctx, cancelFunc := context.WithTimeout(r.ctx, r.config.ElectionTimeout)
				defer cancelFunc()
				var resp *raftpb.RequestVoteResp
				var err error
				resp, err = r.transport.RequestVote(ctx, serverID, req)
				if err != nil {
					r.logger.Errorf("request vote from %s failed: %s", serverID, err.Error())
					return
				}
				respChan <- resp
			})
		}
	}
	return respChan
}

func (r *RaftNode) quorumNodeSize() int {
	return len(r.servers)/2 + 1
}

func (r *RaftNode) runCandidate() {
	r.logger.Info("become candidate")
	respChan := r.startElection()
	votes := 0
	needVotes := r.quorumNodeSize()
	for r.state == Candidate {
		select {
		case <-r.ctx.Done():
			return
		case resp := <-respChan:
			if resp.VoteGranted {
				votes++
				if votes >= needVotes {
					r.state = Leader
					return
				}
			}
		case rpc := <-r.transport.RecvRPC():
			r.processRPC(rpc)
		case <-time.After(util.RandomDuration(r.config.ElectionTimeout)):
			return
		case future := <-r.applyCh:
			future.Respond(nil, ErrNotLeader)
		}
	}
}

func (r *RaftNode) leaderCtx() func() {
	r.leader = r.localID
	ctx, cancelFunc := context.WithCancel(r.ctx)
	commitment := newcommitment(r.servers)
	followers := make(map[string]*Progress, len(r.servers)-1)
	for s := range r.servers {
		if s != r.localID {
			ctx, cancelFunc := context.WithCancel(ctx)
			followers[s] = &Progress{
				ctx:        ctx,
				commitment: commitment,
				cancelFunc: cancelFunc,
				serverID:   s,
				nextIndex:  1,
				notifyCh:   make(chan struct{}, 1),
			}
		}
	}
	leaderState := &leaderState{
		ctx:                ctx,
		cancelFunc:         cancelFunc,
		commitment:         commitment,
		followers:          followers,
		waitGroup:          wait.Group{},
		inflightingFutures: make(map[uint64]*ApplyFuture),
	}
	for _, f := range leaderState.followers {
		p := f
		leaderState.waitGroup.Start(func() {
			r.runHeartbeat(p)
		})
		leaderState.waitGroup.Start(func() {
			r.runLogReplication(p)
		})
	}
	r.leaderState = leaderState
	noop := &ApplyFuture{
		Entry: &raftpb.LogEntry{
			LogType: raftpb.LogEntry_LogNoop,
		},
	}
	noop.init()
	r.dispatch([]*ApplyFuture{noop})
	return func() {
		r.leaderState.cancelFunc()
		r.leaderState.waitGroup.Wait()
		for _, future := range r.leaderState.inflightingFutures {
			future.Respond(nil, ErrNotLeader)
		}
		r.leaderState = nil
	}
}

func (r *RaftNode) stepdown() {
	r.setState(Follower)
	if r.leaderState != nil {
		r.leaderState.cancelFunc()
	}
}

func (r *RaftNode) checkLeaderLease() {
	expireFollowers := 0
	now := time.Now()
	for _, f := range r.leaderState.followers {
		if now.Sub(f.lastContact) > r.config.ElectionTimeout {
			expireFollowers++
			if expireFollowers >= r.quorumNodeSize() {
				r.logger.Warning("leader lease expired")
				r.stepdown()
				return
			}
		}
	}
}

func (r *RaftNode) runLeader() {
	r.logger.Info("become leader")
	defer r.leaderCtx()()
	leaseTimeout := time.After(r.config.ElectionTimeout)
	for r.state == Leader {
		select {
		case <-leaseTimeout:
			leaseTimeout = time.After(r.config.ElectionTimeout)
			r.checkLeaderLease()
		case <-r.leaderState.ctx.Done():
			return
		case future := <-r.applyCh:
			futures := []*ApplyFuture{future}
		L1:
			for {
				select {
				case f := <-r.applyCh:
					futures = append(futures, f)
				default:
					break L1
				}
			}
			r.dispatch(futures)
		case <-r.leaderState.commitment.commitCh:
			r.commitTo(r.leaderState.commitment.commitIndex)
			r.notifyFollowers()
		}
	}
}

func (r *RaftNode) commitTo(toIndex uint64) {
	r.commitIndex = util.MaxUint64(r.commitIndex, toIndex)
	util.AsyncNotify(r.notifyApplyCh)
	r.logger.Debugf("commit log to %d", r.commitIndex)
}

func (r *RaftNode) dispatch(futures []*ApplyFuture) {
	entries := make([]*raftpb.LogEntry, len(futures))
	for i, future := range futures {
		entry := future.Entry
		entry.Term = r.getCurrentTerm()
		entry.Index = r.lastLogIndex + 1 + uint64(i)

		r.mutex.Lock()
		r.leaderState.inflightingFutures[entry.Index] = future
		r.mutex.Unlock()

		entries[i] = entry

	}
	if err := r.entryStore.AppendEntries(entries); err != nil {
		r.logger.Errorf("append entries failed: %s", err.Error())
		r.stepdown()
		return
	}
	if len(entries) > 0 {
		lastLog := entries[len(entries)-1]
		r.setLastLog(lastLog.Term, lastLog.Index)
		r.leaderState.commitment.SetMatchIndex(r.localID, lastLog.Index)
	}
	r.notifyFollowers()
	r.logger.Debugf("dispatch log to %d", r.lastLogIndex)
}

func (r *RaftNode) notifyFollowers() {
	for _, p := range r.leaderState.followers {
		p.notify()
	}
}

func (r *RaftNode) run() {
	for {
		select {
		case <-r.ctx.Done():
			return
		default:
		}
		switch r.state {
		case Follower:
			r.runFollower()
		case Candidate:
			r.runCandidate()
		case Leader:
			r.runLeader()
		}
	}
}

func NewRaftNode(config RaftConfig, storage store.IStore, transport transport.ITransport) *RaftNode {

	ctx, cancelFunc := context.WithCancel(context.Background())
	servers := make(map[string]struct{}, len(config.Servers))
	for _, s := range config.Servers {
		servers[s] = struct{}{}
	}
	r := &RaftNode{
		raftState: raftState{
			state:   Follower,
			localID: config.LocalID,
			servers: servers,
			leader:  None,
		},
		config: config,

		applyCh:     make(chan *ApplyFuture, config.MaxInflightingEntries),
		committedCh: make(chan *DataFuture, config.MaxInflightingEntries),

		notifyApplyCh: make(chan struct{}, 1),

		waitGroup:  wait.Group{},
		ctx:        ctx,
		cancelFunc: cancelFunc,


		entryStore: storage,
		metaStore:  storage,
		transport:  transport,
	}
	r.restoreMeta()
	r.setupLogger()
	r.waitGroup.Start(transport.Serve)
	r.waitGroup.Start(r.runFSM)
	r.waitGroup.Start(r.run)
	return r
}
