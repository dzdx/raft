package transport

import (
	"github.com/dzdx/raft/raftpb"
	"log"
	"context"
	"google.golang.org/grpc/reflection"
	"net"
	"google.golang.org/grpc"
	"fmt"
)

func NewGRPCTransport(servers map[string]string, localID string) *GRPCTransport {
	localAddr := servers[localID]
	lis, err := net.Listen("tcp", localAddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	handleChan := make(chan *RPC)
	raftApp := &grpcRaftApp{handlerChan: handleChan}
	raftpb.RegisterRaftServiceServer(s, raftApp)
	reflection.Register(s)

	clients := make(map[string]raftpb.RaftServiceClient)
	for serverID, address := range servers {
		if serverID == localID {
			continue
		}
		clients[serverID] = newGRPCClient(address)
	}
	transport := &GRPCTransport{
		clients:  clients,
		server:   s,
		app:      raftApp,
		listener: lis,
	}
	return transport
}

type GRPCTransport struct {
	clients  map[string]raftpb.RaftServiceClient
	server   *grpc.Server
	app      *grpcRaftApp
	listener net.Listener
}

func (t *GRPCTransport) RecvRPC() <-chan *RPC {
	return t.app.handlerChan
}
func (t *GRPCTransport) Serve() {
	if err := t.server.Serve(t.listener); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func (t *GRPCTransport) Shutdown() {
	t.server.GracefulStop()
}

func (t *GRPCTransport) RequestVote(ctx context.Context, serverID string, req *raftpb.RequestVoteReq) (*raftpb.RequestVoteResp, error) {
	client, ok := t.clients[serverID]
	if !ok {
		return nil, fmt.Errorf("no client: %s", serverID)
	}

	ctx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()
	resp, err := client.RequestVote(ctx, req)
	return resp, err
}

func (t *GRPCTransport) AppendEntries(ctx context.Context, serverID string, req *raftpb.AppendEntriesReq) (*raftpb.AppendEntriesResp, error) {
	client := t.clients[serverID]
	ctx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()
	resp, err := client.AppendEntries(ctx, req)
	return resp, err
}

func (t *GRPCTransport) InstallSnapshot(ctx context.Context, serverID string, req *raftpb.InstallSnapshotReq) (*raftpb.InstallSnapshotResp, error) {
	client := t.clients[serverID]
	ctx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()
	resp, err := client.InstallSnapshot(ctx, req)
	return resp, err
}

type grpcRaftApp struct {
	handlerChan chan *RPC
}

func (app *grpcRaftApp) RequestVote(ctx context.Context, req *raftpb.RequestVoteReq) (*raftpb.RequestVoteResp, error) {
	rpc := NewRPC(req)
	select {
	case app.handlerChan <- rpc:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case rpcResp := <-rpc.Response():
		return rpcResp.Resp.(*raftpb.RequestVoteResp), rpcResp.Err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (app *grpcRaftApp) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesReq) (*raftpb.AppendEntriesResp, error) {
	rpc := NewRPC(req)
	select {
	case app.handlerChan <- rpc:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case rpcResp := <-rpc.Response():
		return rpcResp.Resp.(*raftpb.AppendEntriesResp), rpcResp.Err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return nil, nil
}

func (app *grpcRaftApp) InstallSnapshot(ctx context.Context, req *raftpb.InstallSnapshotReq) (*raftpb.InstallSnapshotResp, error) {
	rpc := NewRPC(req)
	select {
	case app.handlerChan <- rpc:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case rpcResp := <-rpc.Response():
		return rpcResp.Resp.(*raftpb.InstallSnapshotResp), rpcResp.Err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return nil, nil
}

func newGRPCClient(address string) raftpb.RaftServiceClient {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	c := raftpb.NewRaftServiceClient(conn)
	return c
}
