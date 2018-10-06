package kvhttp

import (
	"github.com/gin-gonic/gin"
	"github.com/DeanThompson/ginpprof"
	"net/http"
	"log"
	"context"
	"github.com/dzdx/raft"
	"github.com/dzdx/raft/store"
	"github.com/dzdx/raft/transport"
	"github.com/dzdx/raft/util/wait"
)

type Node struct {
	http      *http.Server
	router    *gin.Engine
	kvstore   *KvStore
	config    NodeConfig
	waitGroup wait.Group
}

func (n *Node) getKey(c *gin.Context) {
	key := c.Param("key")
	value, err := n.kvstore.GetKey(c, key)
	if err != nil {
		if _, ok := err.(*ErrKeyNotFound); ok {
			c.String(http.StatusNotFound, err.Error())
		} else {
			c.String(http.StatusInternalServerError, err.Error())
		}
	} else {
		c.String(http.StatusOK, value)
	}
}

func (n *Node) putKey(c *gin.Context) {
	key := c.Param("key")
	var data []byte
	var err error
	data, err = c.GetRawData()
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
	}
	value := string(data)
	var resp string
	resp, err = n.kvstore.PutKey(c, key, value)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
	} else {
		c.String(http.StatusOK, resp)
	}
}

func (n *Node) deleteKey(c *gin.Context) {
	key := c.Param("key")
	var resp string
	var err error
	resp, err = n.kvstore.DeleteKey(c, key)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
	} else {
		c.String(http.StatusOK, resp)
	}
}

func (n *Node) confChange(c *gin.Context) {

}

func (n *Node) Run() {
	n.waitGroup.Start(n.kvstore.run)
	if err := n.http.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("listen: %s \n", err)
	}
}
func (n *Node) Shutdown() {
	ctx, cancel := context.WithCancel(context.Background())
	n.http.Shutdown(ctx)
	cancel()
}

func (n *Node) registerHandlers() {
	n.router.GET("/kv/:key", n.getKey)
	n.router.PUT("/kv/:key", n.putKey)
	n.router.DELETE("/kv/:key", n.deleteKey)
	n.router.POST("/", n.confChange)
}

type NodeConfig struct {
	Raftaddrs map[string]string
	Webaddrs  map[string]string
	LocalID   string
}

func newRaftNode(config NodeConfig) *raft.RaftNode {
	servers := make([]string, 0, len(config.Raftaddrs))
	for ID := range config.Raftaddrs {
		servers = append(servers, ID)
	}

	raftConfig := raft.DefaultConfig(servers, config.LocalID)
	//raftConfig.VerboseLog = true
	storage := store.NewInmemStore()
	trans := transport.NewGRPCTransport(config.Raftaddrs, config.LocalID)
	node := raft.NewRaftNode(raftConfig, storage, trans)
	return node
}

func NewNode(config NodeConfig) *Node {

	router := gin.New()
	gin.SetMode(gin.ReleaseMode)
	router.Use(gin.Recovery())
	router.Use(gin.Logger())
	ginpprof.Wrapper(router)

	webaddr := config.Webaddrs[config.LocalID]
	srv := &http.Server{
		Addr:    webaddr,
		Handler: router,
	}
	raftNode := newRaftNode(config)
	kvstore := newKvStore(raftNode)
	node := &Node{
		http:      srv,
		router:    router,
		kvstore:   kvstore,
		waitGroup: wait.Group{},
	}
	node.registerHandlers()
	return node
}
