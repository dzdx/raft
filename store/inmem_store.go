package store

import (
	"github.com/dzdx/raft/raftpb"
	"github.com/dzdx/raft/util"
	"sync"
)

type InmemStore struct {
	mutex     sync.Mutex
	entries   map[uint64]raftpb.LogEntry
	lowIndex  uint64
	highIndex uint64
	kv        map[string][]byte
}

func NewInmemStore() *InmemStore {
	return &InmemStore{
		entries: make(map[uint64]raftpb.LogEntry),
		kv:      make(map[string][]byte),
	}
}

func (s *InmemStore) AppendEntries(es []*raftpb.LogEntry) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	for _, entry := range es {
		s.entries[entry.Index] = *entry
		if s.lowIndex == 0 {
			s.lowIndex = entry.Index
		}
		s.highIndex = util.MaxUint64(s.highIndex, entry.Index)
	}
	return nil
}

func (s *InmemStore) GetEntries(start, end uint64) ([]*raftpb.LogEntry, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	entrys := make([]*raftpb.LogEntry, end-start+1)
	for i := start; i <= end; i++ {
		e, ok := s.entries[i]
		if !ok {
			return nil, NewErrNotFound("entry not found: %v", i)
		}
		entrys[i-start] = &e
	}
	return entrys, nil
}

func (s *InmemStore) GetEntry(index uint64) (*raftpb.LogEntry, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if e, ok := s.entries[index]; !ok {
		return nil, NewErrNotFound("entry not found: %v", index)
	} else {
		return &e, nil
	}
}

func (s *InmemStore) DeleteEntries(start, end uint64) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	for i := start; i <= end; i++ {
		delete(s.entries, i)
	}
	if start <= s.lowIndex {
		s.lowIndex = end + 1
	}
	if end > s.highIndex {
		s.highIndex = start - 1
	}
	if s.lowIndex > s.highIndex {
		s.lowIndex = 0
		s.highIndex = 0
	}
	return nil
}

func (s *InmemStore) FirstIndex() (uint64, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.lowIndex, nil
}

func (s *InmemStore) LastIndex() (uint64, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.highIndex, nil
}

func (s *InmemStore) SetKV(key string, value []byte) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	dst := make([]byte, len(value))
	copy(dst, value)
	s.kv[key] = dst
	return nil
}

func (s *InmemStore) GetKV(key string) ([]byte, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if value, ok := s.kv[key]; !ok {
		return nil, NewErrNotFound("key not found: %s", key)
	} else {
		dst := make([]byte, len(value))
		copy(dst, value)
		return dst, nil
	}
}
