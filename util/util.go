package util

import (
	"time"
	"math/rand"
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

func init() {
	rand.Seed(int64(time.Now().Nanosecond()))
}
