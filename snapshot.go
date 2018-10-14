package raft

import (
	"io"
	"time"
)

type snapshotFuture struct {
	future
}

type snapshotResp struct {
	reader io.ReadCloser
	index  uint64
	term   uint64
}

func (r *RaftNode) runSnapshot() {
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-time.After(r.config.SnapshotInterval):
		case <-r.notifySnapshotCh:
		}
		r.takeSnapshot()
	}
}

func (r *RaftNode) canSnapshot() {
}

func (r *RaftNode) takeSnapshot() error {
	req := snapshotFuture{}
	req.init()
	select {
	case r.fsmSnapshotCh <- req:
	case <-r.ctx.Done():
		return r.ctx.Err()
	}
	var resp RespWithError
	select {
	case resp = <-req.Response():
	case <-r.ctx.Done():
		return r.ctx.Err()
	}
	if resp.Err != nil {
		return nil
	}
	snapshotResp := resp.Resp.(*snapshotResp)
	entry, err := r.snapshoter.Create(snapshotResp.term, snapshotResp.index)
	if err != nil {
		entry.Cancel()
		return err
	}
	if _, err := io.Copy(entry, snapshotResp.reader); err != nil {
		entry.Cancel()
		return err
	}
	if err := entry.Close(); err != nil {
		entry.Cancel()
		return err
	}
	return r.compactLog(snapshotResp.index)
}

func (r *RaftNode) compactLog(end uint64) error {
	start := r.entryStore.FirstIndex()
	return r.entryStore.DeleteEntries(start, end)
}
