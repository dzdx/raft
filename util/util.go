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

func BlockForever() <-chan time.Time {
	return make(chan time.Time)
}

func AtOnce() <-chan time.Time {
	ch := make(chan time.Time)
	close(ch)
	return ch
}

func BackoffTimeout(base time.Duration, backoff int) <-chan time.Time {
	return time.After(base * time.Duration(2^(backoff-1)))
}

func init() {
	rand.Seed(int64(time.Now().Nanosecond()))
}
