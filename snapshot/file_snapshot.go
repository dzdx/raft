package snapshot

type FileSnapShotEntry struct {
	store *FileSnapshotStore
	meta  *SnapShotMeta
}

func (e *FileSnapShotEntry) ID() string {
	return e.meta.ID
}
func (e *FileSnapShotEntry) Write(data []byte) (int, error) {
	return 0, nil
}
func (e *FileSnapShotEntry) WriteAt(data []byte, offset int64) (int, error) {
	return 0, nil
}
func (e *FileSnapShotEntry) Close() error {
	return nil
}

func (e *FileSnapShotEntry) Cancel() error {
	return nil
}

type FileSnapshotStore struct {
	dir string
}

func (s *FileSnapshotStore) Create(term uint64, index uint64) (ISnapShotEntry, error) {
	return nil, nil
}
func (s *FileSnapshotStore) Last() *SnapShotMeta {
	return nil
}

func (s *FileSnapshotStore) Open(ID string) (ISnapShotEntry, error) {
	return nil, nil
}
func (s *FileSnapshotStore) Doing() ISnapShotEntry {
	return nil
}
func (s *FileSnapshotStore) load() error {
	return nil
}

func NewFileSnapshotStore(dir string) *FileSnapshotStore {
	return nil
}
