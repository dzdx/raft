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
			if r.canSnapshot() {
				r.takeSnapshot()
			}
		case <-r.notifySnapshotCh:
			r.takeSnapshot()
		}
	}
}

func (r *RaftNode) canSnapshot() bool {
	first, err := r.entryStore.FirstIndex()
	if err != nil {
		r.logger.Error(err)
		return false
	}
	if r.lastApplied < first {
		r.logger.Fatalf("last applied < first log")
	}
	if first > 0 && r.lastApplied-first > uint64(r.config.SnapshotThreshold) {
		return true
	}
	return false
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
	if _, err := io.CopyBuffer(entry, snapshotResp.reader, make([]byte, 64*2<<10)); err != nil {
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
	start, err := r.entryStore.FirstIndex()
	if err != nil {
		return err
	}
	return r.entryStore.DeleteEntries(start, end)
}
