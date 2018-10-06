package raft

import (
	"github.com/dzdx/raft/util"
	"github.com/dzdx/raft/raftpb"
	"context"
	"time"
)

type Progress struct {
	ctx         context.Context
	commitment  *commitment
	cancelFunc  context.CancelFunc
	serverID    string
	nextIndex   uint64
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
			ctx, cancel := context.WithTimeout(p.ctx, 3*time.Second)
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

func (r *RaftNode) runLogReplication(p *Progress) {
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-p.notifyCh:
			var req *raftpb.AppendEntriesReq
			var resp *raftpb.AppendEntriesResp
			var err error
			if req, err = r.setupAppendEntriesReq(p); err != nil {
				r.logger.Errorf("setup append entries failed: %s", err.Error())
				continue
			}
			if len(req.Entries) == 0 {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
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
					p.commitment.SetMatchIndex(p.serverID, req.Entries[len(req.Entries)-1].Index)
				} else {
					p.nextIndex = util.MinUint64(p.nextIndex-1, resp.LastLogIndex+1)
				}
			}
		}
	}
}

func (r *RaftNode) setupAppendEntriesReq(p *Progress) (*raftpb.AppendEntriesReq, error) {
	start := p.nextIndex
	end := util.MinUint64(r.lastLogIndex, start+uint64(r.config.MaxBatchAppendEntries)-1)
	var entries []*raftpb.LogEntry
	var prevLog *raftpb.LogEntry
	var err error
	if entries, err = r.entryStore.GetEntries(start, end); err != nil {
		return nil, err
	}
	var prevLogIndex, prevLogTerm uint64
	if p.nextIndex-1 > 0 {
		if prevLog, err = r.entryStore.GetEntry(p.nextIndex - 1); err != nil {
			return nil, err
		}
		prevLogIndex = prevLog.Index
		prevLogTerm = prevLog.Term
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