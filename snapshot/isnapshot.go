package snapshot

import (
	"fmt"
	"io"
	"time"
)

type SnapShotMeta struct {
	ID    string
	Term  uint64
	Index uint64
}

type ISnapShotEntry interface {
	io.WriteCloser
	Content() io.ReadCloser
	Cancel() error
	ID() string
}

func snapShotName(term uint64, index uint64) string {
	return fmt.Sprintf("%d-%d-%d", term, index, time.Now().UnixNano()/int64(time.Millisecond))
}

type ISnapShotStore interface {
	Create(term uint64, index uint64) (ISnapShotEntry, error)
	Last() *SnapShotMeta
	Open(ID string) (ISnapShotEntry, error)
}
