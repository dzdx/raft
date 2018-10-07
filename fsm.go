package raft

import (
	"github.com/dzdx/raft/util"
	"github.com/dzdx/raft/raftpb"
)

const maxBatchApply = 64

func splitToFutures(entry *raftpb.LogEntry) (*DataFuture, *IndexFuture) {
	respChan := make(chan *RespWithError, 1)
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

func (r *RaftNode) runFSM() {
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-r.notifyApplyCh:
		}
		for r.lastApplied < r.commitIndex {
			start := r.lastApplied + 1
			end := util.MinUint64(r.commitIndex, r.lastApplied+maxBatchApply)
			entries, err := r.entryStore.GetEntries(start, end)
			if err != nil {
				r.logger.Errorf("get entries failed: %s", err.Error())
				continue
			}
			count := len(entries)
			var i = 0
			for i < count {
				futures := make([]*IndexFuture, 0, count)
			batchApply:
				for i < count {
					entry := entries[i]
					dataFuture, indexFuture := splitToFutures(entry)
					select {
					case r.committedCh <- dataFuture:
					default:
						break batchApply
					}
					futures = append(futures, indexFuture)
					i++
				}
				for _, future := range futures {
					select {
					case <-r.ctx.Done():
						return
					case resp := <-future.Response():
						index := future.Index
						r.lastApplied = index
						if r.leaderState != nil {
							// leader respond request
							reqFuture, ok := r.leaderState.inflightingFutures[index]
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
		}
		r.logger.Debugf("applied log to %d", r.lastApplied)
	}
}
