package raft

import (
	"github.com/dzdx/raft/raftpb"
	"context"
	"github.com/dzdx/raft/util"
	"io"
)

type IFsm interface {
	Apply(context.Context, []DataFuture)
	Snapshot(context.Context) (io.ReadCloser, error)
	Restore(context.Context, io.ReadCloser) error
}

func entryToFutures(entry *raftpb.LogEntry) (*DataFuture, *IndexFuture) {
	respChan := make(chan RespWithError, 1)
	dataFuture := &DataFuture{
		Data: entry.Data,
		future: future{
			respChan: respChan,
		},
	}
	indexFuture := &IndexFuture{
		Index: entry.Index,
		future: future{
			respChan: respChan,
		},
	}
	return dataFuture, indexFuture
}

func (r *RaftNode) batchApplyToFSM() {
	for r.lastApplied < r.commitIndex {
		start := r.lastApplied + 1
		end := util.MinUint64(r.commitIndex, r.lastApplied+uint64(r.config.MaxBatchApplyEntries))
		entries, err := r.entryStore.GetEntries(start, end)
		if err != nil {
			r.logger.Errorf("get entries failed: %s", err.Error())
			continue
		}
		count := len(entries)
		futures := make([]IndexFuture, 0, count)
		applyFutures := make([]DataFuture, 0, count)
		for _, e := range entries {
			dataFuture, indexFuture := entryToFutures(e)
			futures = append(futures, *indexFuture)
			switch e.LogType {
			case raftpb.LogEntry_LogNoop:
				indexFuture.Respond(nil, nil)
			case raftpb.LogEntry_LogCommand:
				applyFutures = append(applyFutures, *dataFuture)
			case raftpb.LogEntry_LogConf:
				indexFuture.Respond(nil, nil)
			}
		}
		r.waitGroup.Start(func() {
			r.fsm.Apply(r.ctx, applyFutures)
		})
		for _, future := range futures {
			select {
			case <-r.ctx.Done():
				return
			case resp := <-future.Response():
				index := future.Index
				r.lastApplied = index
				if r.leaderState != nil {
					// leader respond request
					r.mutex.Lock()
					reqFuture, ok := r.leaderState.inflightingFutures[index]
					r.mutex.Unlock()

					if ok {
						reqFuture.Respond(resp.Resp, resp.Err)

						r.mutex.Lock()
						delete(r.leaderState.inflightingFutures, index)
						r.mutex.Unlock()
					}
				}
			}
		}
	}
	r.logger.Debugf("applied log to %d", r.lastApplied)
}

func (r *RaftNode) runFSM() {
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-r.notifyApplyCh:
			r.batchApplyToFSM()
		case req := <-r.fsmSnapshotCh:
			r.fsmSnapshot(&req)
		}
	}
}

func (r *RaftNode) fsmSnapshot(req *snapshotFuture) {
	reader, err := r.fsm.Snapshot(r.ctx)
	entry, err := r.entryStore.GetEntry(r.lastApplied)
	if err != nil {
		req.Respond(nil, err)
		return
	}

	resp := &snapshotResp{
		index:  r.lastApplied,
		term:   entry.Term,
		reader: reader,
	}
	req.Respond(resp, err)
}
