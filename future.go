package raft

import (
	"github.com/dzdx/raft/raftpb"
	"context"
)

type RespWithError struct {
	Resp interface{}
	Err  error
}

type future struct {
	respChan chan *RespWithError
}

func (future *future) init() {
	future.respChan = make(chan *RespWithError, 1)
}

func (future *future) Respond(resp interface{}, err error) {
	select {
	case future.respChan <- &RespWithError{
		Resp: resp,
		Err:  err,
	}:
	default:
	}
}
func (future *future) Response() <-chan *RespWithError {
	return future.respChan
}

type ApplyFuture struct {
	future
	Entry      *raftpb.LogEntry
	ctx        context.Context
}
type DataFuture struct {
	future
	Data []byte
}

type IndexFuture struct {
	future
	Index uint64
}
