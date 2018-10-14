package raft

import (
	"github.com/dzdx/raft/util"
	"github.com/dzdx/raft/raftpb"
	"context"
	"time"
	"github.com/dzdx/raft/store"
	"io"
)

type progressState int

const (
	syncReplication progressState = iota
	sendSnapshot
)

type Progress struct {
	state       progressState
	ctx         context.Context
	commitment  *commitment
	cancelFunc  context.CancelFunc
	serverID    string
	nextIndex   uint64
	commitIndex uint64
	notifyCh    chan struct{}
	lastContact time.Time
}

func (r *RaftNode) runHeartbeat(p *Progress) {
	ticker := time.NewTicker(r.config.ElectionTimeout / 10)
	defer ticker.Stop()
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			var resp *raftpb.AppendEntriesResp
			var err error
			req := &raftpb.AppendEntriesReq{
				Term:     r.getCurrentTerm(),
				LeaderID: r.localID,
			}
			ctx, cancel := context.WithTimeout(p.ctx, 50*time.Millisecond)
			resp, err = r.transport.AppendEntries(ctx, p.serverID, req)
			cancel()
			if err != nil {
			} else {
				if resp.Term > r.getCurrentTerm() {
					r.setCurrentTerm(resp.Term)
					r.stepdown()
				}
				if resp.Success {
					p.lastContact = time.Now()
				}
			}
		}
	}
}
func (r *RaftNode) syncReplication(p *Progress) {

	failures := 0
	baseTimeout := 10 * time.Millisecond
	for p.state == syncReplication {
		if failures > 0 {
			timeout := time.After(util.MinDuration(util.BackoffDuration(baseTimeout, failures), r.config.MaxReplicationBackoffTimeout))
			select {
			case <-p.ctx.Done():
				return
			case <-timeout:
			}
		}

		var req *raftpb.AppendEntriesReq
		var resp *raftpb.AppendEntriesResp
		var err error
		if req, err = r.setupAppendEntriesReq(p); err != nil {
			failures++
			continue
		}
		ctx, cancel := context.WithTimeout(p.ctx, 3*time.Second)
		resp, err = r.transport.AppendEntries(ctx, p.serverID, req)
		cancel()
		if err != nil {
			r.logger.Errorf("replicate log to %s failed: %s", p.serverID, err.Error())
			failures++
			continue
		}
		failures = 0
		if resp.Term > r.getCurrentTerm() {
			r.setCurrentTerm(resp.Term)
			r.stepdown()
		}
		if resp.Success {
			p.lastContact = time.Now()
			p.commitIndex = req.LeaderCommitIndex
			if len(req.Entries) > 0 {
				lastEntry := req.Entries[len(req.Entries)-1]
				p.commitment.SetMatchIndex(p.serverID, lastEntry.Index)
				p.nextIndex = lastEntry.Index + 1
				r.logger.Debugf("replicated logs [:%d] to %s", lastEntry.Index, p.serverID)
			}

			if p.nextIndex > r.leaderState.dispatchedIndex {
				return
			}
		} else {
			p.nextIndex = util.MinUint64(p.nextIndex-1, resp.LastLogIndex+1)
		}
	}
}

func (r *RaftNode) syncReplicationLoop(p *Progress) {
	r.logger.Debugf("start sync replication to %s", p.serverID)
	defer r.logger.Debugf("stop sync replication to %s", p.serverID)

	for p.state == syncReplication {
		select {
		case <-p.ctx.Done():
			return
		case <-p.notifyCh:
		}
		r.syncReplication(p)
	}
}

func (r *RaftNode) runLogReplication(p *Progress) {
	for {
		select {
		case <-p.ctx.Done():
			return
		default:
		}
		switch p.state {
		case syncReplication:
			r.syncReplicationLoop(p)
		case sendSnapshot:
			r.sendSnapshot(p)
		}
	}
}

func (r *RaftNode) sendSnapshot(p *Progress) {
	r.logger.Debugf("start send snapshot to %s", p.serverID)
	defer func() {
		r.logger.Debugf("end send snapshot to %s", p.serverID)
	}()
	snapMeta := r.snapshoter.Last()
	if snapMeta == nil {
		r.logger.Fatalf("snapshot is empty")
	}
	snap, err := r.snapshoter.Open(snapMeta.ID)
	if err != nil {
		r.logger.Error(err)
		return
	}
	var offset uint64
	for {
		select {
		case <-p.ctx.Done():
			return
		default:
		}
		var done bool
		buffer := make([]byte, 2<<16)
		n, err := snap.Content().Read(buffer)
		if err != nil {
			if err == io.EOF {
				done = true
			} else {
				r.logger.Error(err)
				return
			}
		}
		req := &raftpb.InstallSnapshotReq{
			Term:      r.currentTerm,
			LeaderID:  r.localID,
			LastIndex: snapMeta.Index,
			LastTerm:  snapMeta.Term,
			Offset:    offset,
			Data:      buffer[:n],
			Done:      done,
		}
		select {
		case <-p.ctx.Done():
			return
		default:
		}
		ctx, cancel := context.WithTimeout(p.ctx, 3*time.Second)
		resp, err := r.transport.InstallSnapshot(ctx, p.serverID, req)
		cancel()
		if err != nil {
			r.logger.Error(err)
			return
		}
		if resp.Term > r.getCurrentTerm() {
			r.setCurrentTerm(resp.Term)
			r.stepdown()
			return
		}
		if resp.Success {
			p.lastContact = time.Now()
			if done {
				p.commitment.SetMatchIndex(p.serverID, snapMeta.Index)
				p.nextIndex = snapMeta.Index + 1
				p.state = syncReplication
				return
			}
			offset += uint64(n)
		} else {
			r.logger.Warningf("send snapshot to %s failed", p.serverID)
			return
		}
	}
}

func (r *RaftNode) setupAppendEntriesReq(p *Progress) (*raftpb.AppendEntriesReq, error) {
	start := p.nextIndex
	end := util.MinUint64(r.leaderState.dispatchedIndex, start+uint64(r.config.MaxBatchAppendEntries)-1)
	var entries []*raftpb.LogEntry
	var prevLog *raftpb.LogEntry
	var err error
	if entries, err = r.entryStore.GetEntries(start, end); err != nil {
		if _, ok := err.(*store.ErrNotFound); ok {
			p.state = sendSnapshot
		}
		r.logger.Error(err)
		return nil, err
	}
	var prevLogIndex, prevLogTerm uint64
	if p.nextIndex-1 > 0 {
		snapMeta := r.snapshoter.Last()
		if snapMeta != nil && snapMeta.Index == p.nextIndex-1 {
			prevLogTerm = snapMeta.Term
			prevLogIndex = snapMeta.Index
		} else {
			if prevLog, err = r.entryStore.GetEntry(p.nextIndex - 1); err != nil {
				r.logger.Error(err)
				return nil, err
			}
			prevLogIndex = prevLog.Index
			prevLogTerm = prevLog.Term
		}
	}

	req := &raftpb.AppendEntriesReq{
		Term:              r.getCurrentTerm(),
		LeaderID:          r.localID,
		LeaderCommitIndex: r.commitIndex,
		PrevLogIndex:      prevLogIndex,
		PrevLogTerm:       prevLogTerm,
		Entries:           entries,
	}
	return req, nil
}

func (p *Progress) notify() {
	util.AsyncNotify(p.notifyCh)
}
