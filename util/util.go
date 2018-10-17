package util

import (
	"time"
	"math/rand"
	"encoding/binary"
	"github.com/dzdx/raft/raftpb"
)

func AsyncNotify(c chan struct{}) {
	select {
	case c <- struct{}{}:
	default:
	}
}

func MinUint64(i, j uint64) uint64 {
	if i < j {
		return i
	}
	return j
}
func MaxUint64(i, j uint64) uint64 {
	if i > j {
		return i
	}
	return j
}
func RandomDuration(upperLimit time.Duration) time.Duration {
	return time.Duration((rand.Float64() + 1.0) * float64(upperLimit))
}

func BlockForever() <-chan time.Time {
	return make(chan time.Time)
}

func AtOnce() <-chan time.Time {
	ch := make(chan time.Time)
	close(ch)
	return ch
}

func BackoffDuration(base time.Duration, backoff int) time.Duration {
	return base * time.Duration(2^(backoff-1))
}
func MinDuration(t1 time.Duration, t2 time.Duration) time.Duration {
	if t1 > t2 {
		return t2
	}
	return t1
}

func BytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}
func Uint64ToBytes(i uint64) []byte {
	var b = make([]byte, 8)
	binary.BigEndian.PutUint64(b, i)
	return b
}
func NodesToIDs(nodes []*raftpb.Node) map[string]struct{} {
	serverIDs := make(map[string]struct{}, len(nodes))
	for _, n := range nodes {
		serverIDs[n.ServerID] = struct{}{}
	}
	return serverIDs
}

func init() {
	rand.Seed(int64(time.Now().Nanosecond()))
}
