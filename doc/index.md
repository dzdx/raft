# Learn raft from zero

raft 是一个广泛应用的分布式一致性协议，可以做到CAP定理中的CP。raft由leader election、append entries、membership change、snapshot
 这四大部分构成，本文参考 Diego Ongaro 的论文 [thesis](https://ramcloud.stanford.edu/~ongaro/thesis.pdf)  进行实现。
 
## 目标读者
阅读过raft的论文，并对实现一个可用的raft lib很感兴趣的人

## 技术使用
- [Go](https://golang.org/)
- [gRPC](https://grpc.io/)


## 目录
- leader election
- append entries 

