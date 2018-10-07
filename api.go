package raft

import (
	"github.com/dzdx/raft/raftpb"
	"context"
	"time"
)

type RaftConfig struct {
	MaxInflightingEntries int
	MaxBatchAppendEntries int
	MaxBatchApplyEntries  int
	ElectionTimeout       time.Duration
	Servers               []string
	LocalID               string
	VerboseLog            bool
}

func DefaultConfig(servers []string, localID string) RaftConfig {
	return RaftConfig{
		MaxInflightingEntries: 2048,
		MaxBatchAppendEntries: 64,
		MaxBatchApplyEntries:  64,
		ElectionTimeout:       300 * time.Millisecond,
		Servers:               servers,
		LocalID:               localID,
		VerboseLog:            false,
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

func (r *RaftNode) GetLeader() string {
	return r.leader
}

func (r *RaftNode) CheckQuit() <-chan struct{} {
	return r.ctx.Done()
}
