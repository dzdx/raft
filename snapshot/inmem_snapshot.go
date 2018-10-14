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
	content *bytes.Buffer
}

func (e *InmemSnapshotEntry) ID() string {
	return e.meta.ID
}

func (e *InmemSnapshotEntry) Write(data []byte) (int, error) {
	n, err := io.Copy(e.content, bytes.NewReader(data))
	return int(n), err
}
func (e *InmemSnapshotEntry) Close() error {
	e.store.latest = e
	return nil
}

func (e *InmemSnapshotEntry) Cancel() error {
	return nil
}

func (e *InmemSnapshotEntry) Content() io.ReadCloser {
	return ioutil.NopCloser(e.content)
}

func NewInmemSnapShotStore() *InmemSnapshotStore {
	return &InmemSnapshotStore{
		latest: nil,
	}
}

type InmemSnapshotStore struct {
	latest *InmemSnapshotEntry
}

func (s *InmemSnapshotStore) Create(term uint64, index uint64) (ISnapShotEntry, error) {
	name := snapShotName(term, index)
	entry := &InmemSnapshotEntry{
		meta: &SnapShotMeta{
			ID:    name,
			Term:  term,
			Index: index,
		},
		content: &bytes.Buffer{},
		store:   s,
	}
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
