package kvhttp

import (
	"github.com/dzdx/raft"
	"context"
	"github.com/dzdx/raft/util/wait"
	"github.com/dzdx/raft/raftexample/examplepb"
	"github.com/golang/protobuf/proto"
	"fmt"
)

type ErrKeyNotFound struct {
	msg string
}

func (e *ErrKeyNotFound) Error() string {
	return e.msg
}

type KvStore struct {
	raftNode   *raft.RaftNode
	ctx        context.Context
	cancelFunc context.CancelFunc
	waitGroup  wait.Group
	kv         map[string]string
}

func newKvStore(raftNode *raft.RaftNode) *KvStore {
	ctx, cancelFunc := context.WithCancel(context.Background())
	k := &KvStore{
		ctx:        ctx,
		cancelFunc: cancelFunc,
		waitGroup:  wait.Group{},
		kv:         make(map[string]string),
		raftNode:   raftNode,
	}
	return k
}

func (k *KvStore) WithContext(ctx context.Context) {
	k.cancelFunc()
	ctx, cancelFunc := context.WithCancel(ctx)
	k.ctx = ctx
	k.cancelFunc = cancelFunc
}

func (k *KvStore) run() {
	k.waitGroup.Start(k.runApply)
}

func (k *KvStore) applyData(data []byte) (interface{}, error) {
	kv := &examplepb.KV{}
	if err := proto.Unmarshal(data, kv); err != nil {
		return nil, err
	}
	switch kv.OpType {
	case examplepb.KV_OpPut:
		k.kv[kv.Key] = kv.Value
	case examplepb.KV_OpDelete:
		delete(k.kv, kv.Key)
	}
	return "success", nil
}

func (k *KvStore) runApply() {
	for {
		select {
		case future := <-k.raftNode.CommittedChan():
			future.Respond(k.applyData(future.Data))
		}
	}
}

func (k *KvStore) GetKey(ctx context.Context, key string) (string, error) {
	if value, ok := k.kv[key]; ok {
		return value, nil
	} else {
		return "", &ErrKeyNotFound{fmt.Sprintf("key not found: %s", key)}
	}
}

func (k *KvStore) DeleteKey(ctx context.Context, key string) (string, error) {
	kv := &examplepb.KV{
		Key:    key,
		OpType: examplepb.KV_OpDelete,
	}
	data, _ := proto.Marshal(kv)
	result, err := k.raftNode.Apply(ctx, data)
	return result.(string), err
}

func (k *KvStore) PutKey(ctx context.Context, key, value string) (string, error) {
	kv := &examplepb.KV{
		Key:    key,
		Value:  value,
		OpType: examplepb.KV_OpPut,
	}
	data, _ := proto.Marshal(kv)
	result, err := k.raftNode.Apply(ctx, data)
	return result.(string), err
}
