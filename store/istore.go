package store

import (
	"github.com/dzdx/raft/raftpb"
	"fmt"
)

type ErrNotFound struct {
	s string
}

func NewErrNotFound(format string, args ... interface{}) *ErrNotFound {
	return &ErrNotFound{fmt.Sprintf(format, args...)}
}

func (e *ErrNotFound) Error() string {
	return e.s
}

type IStore interface {
	AppendEntries([]*raftpb.LogEntry) error
	GetEntries(start, end uint64) ([]*raftpb.LogEntry, error)
	GetEntry(index uint64) (*raftpb.LogEntry, error)
	FirstIndex() (uint64, error)
	LastIndex() (uint64, error)
	DeleteEntries(start, end uint64) error
	SetKV(key string, value []byte) error
	GetKV(key string) ([]byte, error)
}
