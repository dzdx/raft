package kvhttp

import (
	"github.com/dzdx/raft"
	"fmt"
	"context"
	"github.com/dzdx/raft/util/wait"
)

type KvService struct {
	raftNode   *raft.RaftNode
	ctx        context.Context
	cancelFunc context.CancelFunc
	waitGroup  wait.Group
}

func newKvService() *KvService {
	ctx, cancelFunc := context.WithCancel(context.Background())
	k := &KvService{
		ctx:        ctx,
		cancelFunc: cancelFunc,
	}
	return k
}

func (k *KvService) WithContext(ctx context.Context) {
	k.cancelFunc()
	ctx, cancelFunc := context.WithCancel(ctx)
	k.ctx = ctx
	k.cancelFunc = cancelFunc
}

func (k *KvService) run() {
	k.waitGroup.Start(k.runApply)
}

func (k *KvService) runApply() {
	for {
		select {
		case future := <-k.raftNode.CommittedChan():
			fmt.Println(future.Data)
			future.Respond("success", nil)
		}
	}
}
