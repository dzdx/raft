package raft

import (
	"github.com/dzdx/raft/raftpb"
	"context"
	"fmt"
)

type RespWithError struct {
	Resp interface{}
	Err  error
}

type future struct {
	respChan chan RespWithError
}

func (future *future) init() {
	future.respChan = make(chan RespWithError, 1)
}

func (future *future) Respond(resp interface{}, err error) {
	select {
	case future.respChan <- RespWithError{
		Resp: resp,
		Err:  err,
	}:
	default:
	}
}
func (future *future) Response() <-chan RespWithError {
	return future.respChan
}

type ApplyFuture struct {
	future
	Entry *raftpb.LogEntry
	ctx   context.Context
}
type DataFuture struct {
	future
	Data []byte
}

type IndexFuture struct {
	future
	Index uint64
}

type ConfChangeFuture struct {
	future
	action *raftpb.ConfChange
}

func (f *ConfChangeFuture) String() string {
	var t string
	if f.action.Type == raftpb.ConfChange_RemoveNode {
		t = "remove"
	} else {
		t = "add"
	}
	var r string
	if f.action.Role == raftpb.NodeRole_Voter {
		r = "voter"
	}
	return fmt.Sprintf("req: %s %s node %s", t, r, f.action.ServerID)
}
