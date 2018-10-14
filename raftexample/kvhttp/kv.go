package kvhttp

import (
	"github.com/dzdx/raft"
	"context"
	"github.com/dzdx/raft/util/wait"
	"github.com/dzdx/raft/raftexample/examplepb"
	"github.com/golang/protobuf/proto"
	"fmt"
	"io"
	"encoding/json"
	"io/ioutil"
	"bytes"
)

type ExampleFSM struct {
	kv *KvStore
}

func (fsm *ExampleFSM) Apply(ctx context.Context, futures []raft.DataFuture) {
	for _, f := range futures {
		select {
		case <-ctx.Done():
			return
		default:
		}
		f.Respond(fsm.kv.applyData(f.Data))
	}
}

func (fsm *ExampleFSM) Restore(ctx context.Context, reader io.ReadCloser) error {
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
	var kv map[string]string
	err = json.Unmarshal(data, kv)
	if err != nil {
		return err
	}
	fsm.kv.kv = kv
	return nil
}

func (fsm *ExampleFSM) Snapshot(ctx context.Context) (io.ReadCloser, error) {
	if data, err := json.Marshal(fsm.kv.kv); err != nil {
		return nil, err
	} else {
		return ioutil.NopCloser(bytes.NewBuffer(data)), nil
	}
}

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

func newKvStore() *KvStore {
	ctx, cancelFunc := context.WithCancel(context.Background())
	k := &KvStore{
		ctx:        ctx,
		cancelFunc: cancelFunc,
		waitGroup:  wait.Group{},
		kv:         make(map[string]string),
	}
	return k
}

func (k *KvStore) FSMFactory() *ExampleFSM {
	return &ExampleFSM{k}
}

func (k *KvStore) WithContext(ctx context.Context) {
	k.cancelFunc()
	ctx, cancelFunc := context.WithCancel(ctx)
	k.ctx = ctx
	k.cancelFunc = cancelFunc
}

func (k *KvStore) SetRaftNode(node *raft.RaftNode) {
	k.raftNode = node
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
	if err != nil {
		return "", err
	}
	return result.(string), nil
}

func (k *KvStore) PutKey(ctx context.Context, key, value string) (string, error) {
	kv := &examplepb.KV{
		Key:    key,
		Value:  value,
		OpType: examplepb.KV_OpPut,
	}
	data, _ := proto.Marshal(kv)
	result, err := k.raftNode.Apply(ctx, data)
	if err != nil {
		return "", err
	}
	return result.(string), nil
}
