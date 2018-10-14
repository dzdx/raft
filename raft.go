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
	"github.com/dzdx/raft/snapshot"
)

var (
	ErrNotLeader = errors.New("not leader")
	ErrNoLeader  = errors.New("no leader")
)

type leaderState struct {
	ctx                context.Context
	cancelFunc         context.CancelFunc
	commitment         *commitment
	followers          map[string]*Progress
	waitGroup          wait.Group
	inflightingFutures map[uint64]ApplyFuture
	dispatchedIndex    uint64
}

type RaftNode struct {
	raftState

	config        RaftConfig
	applyCh       chan ApplyFuture
	committedCh   chan DataFuture
	fsmSnapshotCh chan snapshotFuture

	notifyApplyCh    chan struct{}
	notifySnapshotCh chan struct{}

	waitGroup   wait.Group
	ctx         context.Context
	cancelFunc  context.CancelFunc
	mutex       sync.Mutex
	leaderState *leaderState

	entryStore      store.IStore
	metaStore       store.IStore
	transport       transport.ITransport
	fsm             IFsm
	snapshoter      snapshot.ISnapShotStore
	currentSnapshot string

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
	case *raftpb.InstallSnapshotReq:
		r.processInstallSnapshot(rpc, req)
	}
}

func (r *RaftNode) lastIndex() uint64 {
	_, index := r.getLastLog()
	return index
}

func (r *RaftNode) processInstallSnapshot(rpc *transport.RPC, req *raftpb.InstallSnapshotReq) {
	resp := &raftpb.InstallSnapshotResp{Success: false}
	defer func() {
		resp.Term = r.getCurrentTerm()
		rpc.Respond(resp, nil)
	}()
	var err error
	var entry snapshot.ISnapShotEntry
	if req.Offset == 0 {
		if entry, err = r.snapshoter.Create(req.LastTerm, req.LastIndex); err != nil {
			r.logger.Error(err)
			return
		}
	} else {
		if entry, err = r.snapshoter.Open(r.currentSnapshot); err != nil {
			r.logger.Error(err)
			return
		}
	}
	r.currentSnapshot = entry.ID()
	if _, err = entry.Write(req.Data); err != nil {
		r.logger.Error(err)
		entry.Cancel()
		return
	}
	if err = entry.Close(); err != nil {
		r.logger.Error(err)
		entry.Cancel()
		return
	}
	if req.Done {
		if err = r.fsm.Restore(r.ctx, entry.Content()); err != nil {
			r.logger.Error(err)
			entry.Cancel()
			return
		} else {
			if req.LastIndex > r.lastIndex() {
				r.setLastLog(req.LastTerm, req.LastIndex)
				r.compactLog(req.LastIndex)
				r.lastApplied = req.LastIndex
			}
		}
	}
	r.setState(Follower)
	r.setLastContactLeader(req.LeaderID)
	r.resetElectionTimer()
	resp.Success = true
}

func (r *RaftNode) processAppendEntries(rpc *transport.RPC, req *raftpb.AppendEntriesReq) {
	resp := &raftpb.AppendEntriesResp{
		Success: false,
	}
	defer func() {
		resp.Term = r.getCurrentTerm()
		resp.LastLogIndex = r.lastIndex()
		rpc.Respond(resp, nil)
	}()

	if req.Term < r.currentTerm {
		// reject stale leader's log
		return
	}

	if req.Term > r.currentTerm {
		r.setCurrentTerm(req.Term)
	}
	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex > r.lastIndex() {
			return
		}
		lastSnap := r.snapshoter.Last()
		if lastSnap != nil && lastSnap.Index == req.PrevLogIndex {
			if req.PrevLogTerm != lastSnap.Term {
				return
			}
		} else {
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

		var lastIndex = r.lastIndex()
		defer func() {
			term, index := r.getLastLog()
			if lastLog, err := r.entryStore.GetEntry(lastIndex); err != nil {
				r.logger.Error(err)
				resp.Success = false
			} else {
				if lastLog.Term != term || lastLog.Index != index {
					r.setLastLog(lastLog.Term, lastLog.Index)
				}
			}
		}()
		// delete conflict log entries
		if err := r.entryStore.DeleteEntries(req.Entries[newStart].Index, r.lastIndex()); err != nil {
			r.logger.Errorf("delete entries failed: %s", err.Error())
			return
		}
		if req.Entries[newStart].Index <= r.lastIndex() {
			lastIndex = req.Entries[newStart].Index - 1
		}

		newEntries := req.Entries[newStart:]
		if err := r.entryStore.AppendEntries(newEntries); err != nil {
			r.logger.Errorf("append entries failed: %s", err.Error())
			return
		}
		lastIndex = req.Entries[len(req.Entries)-1].Index
	}

	if req.LeaderCommitIndex > 0 {
		r.commitTo(util.MinUint64(req.LeaderCommitIndex, r.lastIndex()))
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
		// now this node has another leader
		// to avoid
		// Leader -x- F1
		// Leader --- F2
		// F1 --- F2
		return
	}
	if r.lastVotedTerm == req.Term && r.lastVotedFor != None && r.lastVotedFor != req.CandidateID {
		// has voted to other node in this term
		return
	}
	if req.Term < r.currentTerm {
		return
	}
	var lastLogIndex, lastLogTerm uint64
	lastLogIndex = r.lastIndex()
	if lastLogIndex > 0 {
		lastLog, err := r.entryStore.GetEntry(r.lastIndex())
		if err != nil {
			r.logger.Errorf("get entry failed: %s", err.Error())
			return
		}
		lastLogTerm = lastLog.Term
	}
	if req.LastLogTerm < lastLogTerm {
		return
	}
	if req.LastLogTerm == lastLogTerm && req.LastLogIndex < lastLogIndex {
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
	var lastLogIndex, lastLogTerm uint64
	lastLogIndex = r.lastIndex()
	if lastLogIndex > 0 {
		lastLog, err := r.entryStore.GetEntry(r.lastIndex())
		if err != nil {
			r.logger.Error(err)
			return nil
		}
		lastLogTerm = lastLog.Term
	}
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
				nextIndex:  r.lastIndex() + 1,
				notifyCh:   make(chan struct{}, 1),
				state:      syncReplication,
			}
		}
	}
	leaderState := &leaderState{
		ctx:                ctx,
		cancelFunc:         cancelFunc,
		commitment:         commitment,
		followers:          followers,
		waitGroup:          wait.Group{},
		inflightingFutures: make(map[uint64]ApplyFuture),
		dispatchedIndex:    r.lastIndex(),
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
	noop := ApplyFuture{
		Entry: &raftpb.LogEntry{
			LogType: raftpb.LogEntry_LogNoop,
		},
	}
	noop.init()
	r.dispatch([]ApplyFuture{noop})
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
			futures := []ApplyFuture{future}
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
			r.leaderCommit(r.leaderState.commitment.commitIndex)
		}
	}
}
func (r *RaftNode) leaderCommit(toIndex uint64) {
	entry, err := r.entryStore.GetEntry(toIndex)
	if err != nil {
		r.logger.Error(err)
		return
	}
	if entry.Term == r.currentTerm {
		r.commitTo(toIndex)
		r.leaderState.waitGroup.Start(func() {
			select {
			case <-r.leaderState.ctx.Done():
				return
			case <-time.After(util.RandomDuration(r.config.CommitTimeout)):
			}
			for _, p := range r.leaderState.followers {
				if p.commitIndex < r.commitIndex {
					p.notify()
				}
			}
		})
	}
}

func (r *RaftNode) commitTo(toIndex uint64) {
	if toIndex > r.commitIndex {
		r.commitIndex = util.MaxUint64(r.commitIndex, toIndex)
		util.AsyncNotify(r.notifyApplyCh)
		r.logger.Debugf("commit log to %d", r.commitIndex)
	}
}

func (r *RaftNode) dispatch(futures []ApplyFuture) {
	if len(futures) == 0 {
		return
	}
	entries := make([]*raftpb.LogEntry, len(futures))
	dispatchedIndex := r.leaderState.dispatchedIndex
	for i, future := range futures {
		entry := future.Entry
		entry.Term = r.getCurrentTerm()
		dispatchedIndex++
		entry.Index = dispatchedIndex
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
	lastLog := entries[len(entries)-1]
	r.setLastLog(lastLog.Term, lastLog.Index)

	r.leaderState.dispatchedIndex = dispatchedIndex
	if len(entries) > 0 {
		r.leaderState.commitment.SetMatchIndex(r.localID, r.lastIndex())
	}
	for _, p := range r.leaderState.followers {
		p.notify()
	}
	r.logger.Debugf("dispatch log to %d", r.lastIndex())
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

func NewRaftNode(config RaftConfig, storage store.IStore, transport transport.ITransport, fsm IFsm, snapshoter snapshot.ISnapShotStore) *RaftNode {

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

		applyCh:     make(chan ApplyFuture, config.MaxInflightingEntries),
		committedCh: make(chan DataFuture, config.MaxInflightingEntries),

		notifyApplyCh:    make(chan struct{}, 1),
		notifySnapshotCh: make(chan struct{}, 1),
		fsmSnapshotCh:    make(chan snapshotFuture),

		waitGroup:  wait.Group{},
		ctx:        ctx,
		cancelFunc: cancelFunc,


		entryStore: storage,
		metaStore:  storage,
		transport:  transport,
		fsm:        fsm,
		snapshoter: snapshoter,
	}
	r.restoreMeta()
	r.setupLogger()
	r.waitGroup.Start(transport.Serve)
	r.waitGroup.Start(r.runFSM)
	r.waitGroup.Start(r.runSnapshot)
	r.waitGroup.Start(r.run)
	return r
}
