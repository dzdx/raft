package transport

import (
	"github.com/dzdx/raft/raftpb"
	"context"
	"fmt"
	"math/rand"
)

type InmemNetwork struct {
	transports map[string]*InmemTransport
}

func NewInmemNetwork(servers []string) *InmemNetwork {
	num := len(servers)
	transports := make(map[string]*InmemTransport, num)
	for i := 0; i < num; i++ {
		transports[servers[i]] = NewInmemTransport(servers[i])
	}
	for i, tranC := range transports {
		for j, tranS := range transports {
			if i != j {
				tranC.Connect(tranS)
			}
		}
	}
	return &InmemNetwork{transports: transports}
}

func (n *InmemNetwork) GetTrans(serverID string) *InmemTransport {
	return n.transports[serverID]
}
func (n *InmemNetwork) SetPartition(partitions ...[]string) {
	logger.Infof("set network partition %v ", partitions)
	for i, part1 := range partitions {
		for j, part2 := range partitions {
			for _, server1 := range part1 {
				for _, server2 := range part2 {
					if i != j {
						n.transports[server1].SetReliability(server2, 0.0, 0.0)
						n.transports[server2].SetReliability(server1, 0.0, 0.0)
					} else {
						n.transports[server1].SetReliability(server2, 1.0, 1.0)
						n.transports[server2].SetReliability(server1, 1.0, 1.0)
					}
				}
			}
		}
	}
}

func NewInmemTransport(localID string) *InmemTransport {
	return &InmemTransport{
		conns:           make(map[string]*InmemTransport),
		handlerChan:     make(chan *RPC, 256),
		localID:         localID,
		sendReliability: make(map[string]float64),
		recvReliability: make(map[string]float64),
		quit:            make(chan struct{}),
	}
}

type InmemTransport struct {
	conns           map[string]*InmemTransport
	handlerChan     chan *RPC
	localID         string
	quit            chan struct{}
	sendReliability map[string]float64
	recvReliability map[string]float64
}

func (t *InmemTransport) Connect(trans *InmemTransport) {
	t.conns[trans.localID] = trans
	t.sendReliability[trans.localID] = 1.0
	t.recvReliability[trans.localID] = 1.0
}

func (t *InmemTransport) RecvRPC() <-chan *RPC {
	return t.handlerChan
}

func (t *InmemTransport) RequestVote(ctx context.Context, serverID string, req *raftpb.RequestVoteReq) (*raftpb.RequestVoteResp, error) {
	resp, err := t.sendRPC(ctx, serverID, req)
	if err != nil {
		return nil, err
	}
	return resp.(*raftpb.RequestVoteResp), nil
}

func (t *InmemTransport) AppendEntries(ctx context.Context, serverID string, req *raftpb.AppendEntriesReq) (*raftpb.AppendEntriesResp, error) {
	resp, err := t.sendRPC(ctx, serverID, req)
	if err != nil {
		return nil, err
	}
	return resp.(*raftpb.AppendEntriesResp), nil
}

func (t *InmemTransport) readNetworkInfo(serverID string) (sendReliability, recvReliability float64) {
	var ok bool
	if sendReliability, ok = t.sendReliability[serverID]; !ok {
		sendReliability = 1.0
	}
	if recvReliability, ok = t.recvReliability[serverID]; !ok {
		recvReliability = 1.0
	}
	return
}

func (t *InmemTransport) sendRPC(ctx context.Context, serverID string, req interface{}) (interface{}, error) {
	var peer *InmemTransport
	var ok bool

	if peer, ok = t.conns[serverID]; !ok {
		return nil, fmt.Errorf("failed to connect to %s", serverID)
	}
	sendReliability, recvReliability := t.readNetworkInfo(serverID)
	if (1.0 - sendReliability) >= rand.Float64() {
		return nil, fmt.Errorf("send req to %s failed", serverID)
	}
	rpc := NewRPC(req)
	select {
	case peer.handlerChan <- rpc:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case rpcResp := <-rpc.Response():
		if (1.0 - recvReliability) >= rand.Float64() {
			return nil, fmt.Errorf("recv resp from %s failed", serverID)
		}
		return rpcResp.Resp, rpcResp.Err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (t *InmemTransport) Serve() {
	<-t.quit
}
func (t *InmemTransport) Shutdown() {
	close(t.quit)
}

func (t *InmemTransport) SetReliability(serverID string, sendReliability float64, recvReliability float64) {
	t.sendReliability[serverID] = sendReliability
	t.recvReliability[serverID] = recvReliability
}
