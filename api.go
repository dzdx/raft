package raft

import (
	"context"
	"github.com/dzdx/raft/raftpb"
	"time"
	"github.com/dzdx/raft/util"
)

type RaftConfig struct {
	Servers []string
	LocalID string

	MaxInflightingEntries        int
	MaxBatchAppendEntries        int
	MaxBatchApplyEntries         int
	ElectionTimeout              time.Duration
	SnapshotInterval             time.Duration
	SnapshotThreshold            int
	MaxReplicationBackoffTimeout time.Duration
	CommitTimeout                time.Duration

	VerboseLog bool
}

func DefaultConfig(servers []string, localID string) RaftConfig {
	return RaftConfig{
		MaxInflightingEntries:        2048,
		MaxBatchAppendEntries:        64,
		MaxBatchApplyEntries:         64,
		SnapshotInterval:             10 * time.Minute,
		SnapshotThreshold:            2048,
		ElectionTimeout:              300 * time.Millisecond,
		CommitTimeout:                50 * time.Millisecond,
		MaxReplicationBackoffTimeout: 3 * time.Second,
		Servers:                      servers,
		LocalID:                      localID,
		VerboseLog:                   false,
	}
}

func (r *RaftNode) Apply(ctx context.Context, data []byte) (interface{}, error) {
	future := ApplyFuture{
		Entry: &raftpb.LogEntry{
			Data:    data,
			LogType: raftpb.LogEntry_LogCommand,
		},
		ctx: ctx,
	}
	future.init()
	select {
	case r.applyCh <- future:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case respWithError := <-future.Response():
		return respWithError.Resp, respWithError.Err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (r *RaftNode) GetLeader() string {
	return r.leader
}

func (r *RaftNode) Snapshot() {
	util.AsyncNotify(r.notifySnapshotCh)
}
