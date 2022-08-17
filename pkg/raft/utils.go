package raft

import (
	"math/rand"
	"time"
)

func Min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}

func Max(a, b int) int {
	if a <= b {
		return b
	}
	return a
}

func RandomElectionTimeout(baseElectionTimeout uint64) time.Duration {
	source := rand.NewSource(time.Now().UnixNano())
	r := rand.New(source)
	randomTime := r.Intn(int(baseElectionTimeout)) + int(baseElectionTimeout)
	return time.Millisecond * time.Duration(randomTime)
}
