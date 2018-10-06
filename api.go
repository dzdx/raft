package raft

import (
	"github.com/dzdx/raft/raftpb"
	"context"
	"time"
)

func DefaultConfig(servers []string, localID string) RaftConfig {
	return RaftConfig{
		MaxInflightingEntries: 2048,
		MaxBatchAppendEntries: 256,
		ElectionTimeout:       300 * time.Millisecond,
		Servers:               servers,
		LocalID:               localID,
	}
}

func (r *RaftNode) Apply(ctx context.Context, data []byte) (interface{}, error) {
	future := &ApplyFuture{
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

func (r *RaftNode) CommittedChan() <-chan *DataFuture {
	return r.committedCh
}

func (r *RaftNode) GetLeader() {
}
