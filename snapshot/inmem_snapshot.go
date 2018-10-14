package snapshot

import (
	"bytes"
	"io"
	"io/ioutil"
	"fmt"
)

type InmemSnapshotEntry struct {
	store   *InmemSnapshotStore
	meta    *SnapShotMeta
	content []byte
	offset  int64
}

func (e *InmemSnapshotEntry) ID() string {
	return e.meta.ID
}
func (e *InmemSnapshotEntry) Write(data []byte) (int, error) {
	buf := make([]byte, len(e.content)+len(data))
	copy(buf[:len(e.content)], e.content)
	copy(buf[len(e.content):], data)
	e.offset = int64(len(e.content)) + int64(len(data))
	e.content = buf
	return len(data), nil
}

func (e *InmemSnapshotEntry) WriteAt(data []byte, offset int64) (int, error) {
	buf := make([]byte, int(offset)+len(data))
	copy(buf[:len(e.content)], e.content)
	copy(buf[offset:], data)
	e.offset = offset + int64(len(data))
	e.content = buf
	return len(data), nil
}

func (e *InmemSnapshotEntry) Close() error {
	e.store.latest = e
	e.store.doing = nil
	return nil
}

func (e *InmemSnapshotEntry) Cancel() error {
	e.store.doing = nil
	return nil
}

func (e *InmemSnapshotEntry) Content() io.ReadCloser {
	return ioutil.NopCloser(bytes.NewReader(e.content))
}

func NewInmemSnapShotStore() *InmemSnapshotStore {
	return &InmemSnapshotStore{
		latest: nil,
	}
}

type InmemSnapshotStore struct {
	latest *InmemSnapshotEntry
	doing  *InmemSnapshotEntry
}

func (s *InmemSnapshotStore) Create(term uint64, index uint64) (ISnapShotEntry, error) {
	name := snapShotName(term, index)
	entry := &InmemSnapshotEntry{
		meta: &SnapShotMeta{
			ID:    name,
			Term:  term,
			Index: index,
		},
		content: make([]byte, 0),
		store:   s,
	}
	s.doing = entry
	return entry, nil
}

func (s *InmemSnapshotStore) Last() *SnapShotMeta {
	if s.latest == nil {
		return nil
	}
	return s.latest.meta
}

func (s *InmemSnapshotStore) Open(ID string) (ISnapShotEntry, error) {
	if s.latest != nil && s.latest.meta.ID == ID {
		return s.latest, nil
	}
	return nil, fmt.Errorf("no snapshot named %s", ID)
}

func (s *InmemSnapshotStore) Doing() ISnapShotEntry {
	return s.doing
}
