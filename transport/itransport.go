package transport

import (
	"context"
	"github.com/dzdx/raft/raftpb"
	"github.com/sirupsen/logrus"
	"os"
)

var (
	logger *logrus.Logger
)

type RPCResp struct {
	Resp interface{}
	Err  error
}

type RPC struct {
	Req    interface{}
	respCh chan *RPCResp
}

func NewRPC(req interface{}) *RPC {
	return &RPC{
		Req:    req,
		respCh: make(chan *RPCResp, 1),
	}
}

func (rpc *RPC) Respond(resp interface{}, err error) {
	select {
	case rpc.respCh <- &RPCResp{
		Resp: resp,
		Err:  err,
	}:
	default:
	}
}

func (rpc *RPC) Response() <-chan *RPCResp {
	return rpc.respCh
}

type ITransport interface {
	RecvRPC() <-chan *RPC
	RequestVote(ctx context.Context, serverID string, req *raftpb.RequestVoteReq) (*raftpb.RequestVoteResp, error)
	AppendEntries(ctx context.Context, serverID string, req *raftpb.AppendEntriesReq) (*raftpb.AppendEntriesResp, error)
	Serve()
	Shutdown()
}

func init() {
	formatter := &logrus.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	formatter.FullTimestamp = true

	logger = logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	logger.SetFormatter(formatter)
	logger.SetOutput(os.Stdout)
}
