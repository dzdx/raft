package store

import (
	"github.com/dzdx/raft/raftpb"
	"github.com/dzdx/raft/util"
)

type InmemStore struct {
	entrys    map[uint64]raftpb.LogEntry
	lastIndex uint64
	kv        map[string][]byte
}

func NewInmemStore() *InmemStore {
	return &InmemStore{
		entrys: make(map[uint64]raftpb.LogEntry),
		kv:     make(map[string][]byte),
	}
}

func (s *InmemStore) AppendEntries(es []*raftpb.LogEntry) error {
	for _, entry := range es {
		s.entrys[entry.Index] = *entry
		s.lastIndex = util.MaxUint64(s.lastIndex, entry.Index)
	}
	return nil
}

func (s *InmemStore) GetEntries(start, end uint64) ([]*raftpb.LogEntry, error) {
	entrys := make([]*raftpb.LogEntry, end-start+1)
	for i := uint64(0); i < end-start+1; i++ {
		e := s.entrys[start+i]
		entrys[i] = &e
	}
	return entrys, nil
}

func (s *InmemStore) GetEntry(index uint64) (*raftpb.LogEntry, error) {
	if e, ok := s.entrys[index]; !ok {
		return nil, NewErrNotFound("entry not found: %v", index)
	} else {
		return &e, nil
	}
}

func (s *InmemStore) DeleteEntries(start, end uint64) error {
	for i := start; i <= end; i++ {
		delete(s.entrys, i)
	}
	s.lastIndex = start - 1
	return nil
}

func (s *InmemStore) SetKV(key string, value []byte) error {
	dst := make([]byte, len(value))
	copy(dst, value)
	s.kv[key] = dst
	return nil
}

func (s *InmemStore) GetKV(key string) ([]byte, error) {
	if value, ok := s.kv[key]; !ok {
		return nil, NewErrNotFound("key not found: %s", key)
	} else {
		dst := make([]byte, len(value))
		copy(dst, value)
		return dst, nil
	}
}
