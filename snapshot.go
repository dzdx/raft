package raft

import (
	"io"
	"time"
	"fmt"
	"github.com/golang/protobuf/proto"
	"encoding/binary"
	"github.com/dzdx/raft/raftpb"
	"github.com/dzdx/raft/snapshot"
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
	if r.configurations.committed.Index != r.configurations.last.Index {
		return fmt.Errorf("snapshot should wait conf change finished ")
	}

	lastConf := r.configurations.last

	snapshotResp := resp.Resp.(*snapshotResp)
	entry, err := r.snapshoter.Create(snapshotResp.term, snapshotResp.index)
	if err != nil {
		entry.Cancel()
		return err
	}
	confData, _ := proto.Marshal(lastConf)
	var confLen = make([]byte, 4)
	binary.BigEndian.PutUint32(confLen, uint32(len(confData)))
	entry.Write(confLen)
	entry.Write(confData)
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

func (r *RaftNode) restoreSnapshot() {
	last := r.snapshoter.Last()
	if last == nil {
		return
	}
	var snap snapshot.ISnapShotEntry
	var err error
	snap, err = r.snapshoter.Open(last.ID)
	if err != nil {
		r.logger.Fatal(err)
	}
	content := snap.Content()
	var conf *raftpb.Configuration
	if conf, err = r.extractConfiguration(content); err != nil {
		r.logger.Fatal(err)
	}
	if r.configurations.last.Index < conf.Index {
		r.configurations.last = conf
	}
	if err = r.fsm.Restore(r.ctx, content); err != nil {
		r.logger.Fatal(err)
	}
	r.lastApplied = last.Index
}

func (r *RaftNode) extractConfiguration(content io.Reader) (*raftpb.Configuration, error) {
	var confLen = make([]byte, 4)
	content.Read(confLen)
	var confData = make([]byte, binary.BigEndian.Uint32(confLen))
	content.Read(confData)
	var conf = &raftpb.Configuration{}
	if err := proto.Unmarshal(confData, conf); err != nil {
		return nil, err
	}
	return conf, nil
}

func (r *RaftNode) replayLogs() {
	var lastIndex, firstIndex uint64
	var err error
	if lastIndex, err = r.entryStore.LastIndex(); err != nil {
		r.logger.Fatal(err)
	}
	if firstIndex, err = r.entryStore.FirstIndex(); err != nil {
		r.logger.Fatal(err)
	}
	if lastIndex > 0 && firstIndex > 0 {

		for index := lastIndex; index >= firstIndex; index-- {
			entry, err := r.entryStore.GetEntry(index)
			if err != nil {
				r.logger.Fatal(err)
			}
			if entry.LogType == raftpb.LogEntry_LogConf {
				var conf = &raftpb.Configuration{}
				if err := proto.Unmarshal(entry.Data, conf); err != nil {
					r.logger.Fatal(err)
				}
				if r.configurations.last.Index < conf.Index {
					r.configurations.last = conf
				}
				break
			}
		}
	}
}
