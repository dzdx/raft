# start

首先请安装 go >= 1.11 或者 vgo 用于依赖管理

创建一个工作目录

```
mkdir raft
```

安装 grpc-go

```
go get -u github.com/golang/protobuf/{proto,protoc-gen-go}
go get -u google.golang.org/grpc
```

首先完成leader election和heartbeat两项的基础代码，根据thesis P13的图表，定义了以下 protobuf 文件

```
syntax = "proto3";
package raftpb;

message RequestVoteReq{
    uint64 term = 1;
    uint64 lastLogIndex = 2;
    uint64 lastLogTerm = 3;
    string candidateID = 4;
}


message RequestVoteResp{
    uint64 term = 1;
    bool voteGranted = 2;
}

message LogEntry{
    uint64 term = 1;
    uint64 index = 2;
    bytes data = 3;
}

message AppendEntriesReq{
    uint64 term = 1;
    string leaderID = 2;
    uint64 prevLogIndex = 3;
    uint64 prevLogTerm = 4;
    uint64 leaderCommit = 6;
    repeated LogEntry entries = 5;
}

message AppendEntriesResp{
    uint64 term = 1;
    bool success = 2;
}


service RaftService{
    rpc RequestVote(RequestVoteReq) returns (RequestVoteResp);
    rpc AppendEntries(AppendEntriesReq) returns (AppendEntriesResp);
}
```

使用 protoc 生成代码

```
mkdir -p raftpb
protoc --go_out=plugins=grpc:raftpb/ *.proto
```


先定义一下raft struct 需要的基础属性

| name |  介绍 | 持久化 |
|---|---|---|
| lastVotedFor | 上一轮投给了谁 | true |
| lastVotedTerm | 上一轮投票的term | true |
| currentTerm | 当前term | true |
| lastApplied | 已经应用到状态机的log的index | 和状态机是否持久化保持一致 |
| lastLogIndex | 最后一条log的index | 启动时从log的存储中读取 |
| lastLogTerm | 最后一条log的term | 启动时从log的存储中读取 |
| commitIndex | 已经提交的log的index| false |
| state | 当前raft节点的状态(Follower、Candidate、Leader) | false |
| localID | 当前raft节点的ID | 来自配置 |
| servers | raft集群所有的节点的id列表 | 来自配置 |



```
type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
)

type raftState struct {
	lastVotedFor  string
	lastVotedTerm uint64
	currentTerm   uint64
	lastLogIndex  uint64
	lastLogTerm   uint64

	lastApplied uint64
	commitIndex uint64
	state       State
	localID     string
	servers     map[string]struct{}
}

type RaftNode struct {
	raftState
}

```

接下来把存储和网络部分单独抽象出来，方便在unittest中替换

- 存储

```
mkdir store
touch store/istore.go
```
```

var (
	ErrKeyNotFound = errors.New("key not found")
)
type IStore interface {
	AppendEntries([]*raftpb.LogEntry) error
	GetEntries(start, end uint64) ([]*raftpb.LogEntry, error)
	SetKV(key string, value []byte) error
}
```

- 网络

```
mkdir transport
touch transport/itransport.go
```

为了方便在raft中的使用与unittest的编写方便， 添加了RPC对象

```
type RPCResp struct {
	Resp interface{}
	Err  error
}

type RPC struct {
	Req    interface{}
	respCh chan RPCResp
}

func NewRPC(req interface{}) *RPC {
	return &RPC{
		Req:    req,
		respCh: make(chan RPCResp, 1),
	}
}

func (rpc *RPC) Respond(resp interface{}, err error) {
	select {
	case rpc.respCh <- RPCResp{
		Resp: resp,
		Err:  err,
	}:
	default:
	}
}

func (rpc *RPC) Response() <-chan RPCResp {
	return rpc.respCh
}

type ITransport interface {
	RecvRPC() <-chan *RPC
	RequestVote(ctx context.Context, serverID string, req *raftpb.RequestVoteReq) (*raftpb.RequestVoteResp, error)
	AppendEntries(ctx context.Context, serverID string, req *raftpb.AppendEntriesReq) (*raftpb.AppendEntriesResp, error)
	Serve()
	Shutdown()
}
```


