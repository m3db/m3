package storage

import (
	"strconv"
	"testing"

	"go.uber.org/atomic"
)

func benchmarkThrottling(users int, concurrencyPerUser int, totalAcquiresPerUser int64, b *testing.B) {
	t := NewThrottler(100000)

	for user := 0; user < users; user++ {
		userId := strconv.Itoa(user)
		perUserAcquires := atomic.NewInt64(0)
		for routine := 0; routine < concurrencyPerUser; routine++ {
			go func() {
				for perUserAcquires.Inc() < totalAcquiresPerUser {
					claim, _ := t.Acquire(userId)
					claim.Release()
				}
			}()
		}
	}
}

// Single user acquires and releases resources across 1 routine
func BenchmarkSingleUserNoConcurrency(b *testing.B) {
	benchmarkThrottling(1, 1, 100_000_000, b)
}

// Single user acquires and releases resources across 100 routines
func BenchmarkSingleUserSomeConcurrency(b *testing.B) {
	benchmarkThrottling(1, 100, 100_000_000, b)
}

// Single user acquires and releases resources across 10000 routines
func BenchmarkSingleUserHighConcurrency(b *testing.B) {
	benchmarkThrottling(1, 10000, 100_000_000, b)
}

// Many users acquire and release 1M resources across 1 routine each
func BenchmarkManyUsersNoConcurrency(b *testing.B) {
	benchmarkThrottling(100, 1, 100_000_000, b)
}

// Many users acquire and release 1M resources across 10 routines each
func BenchmarkManyUsersSomeConcurrency(b *testing.B) {
	benchmarkThrottling(100, 10, 100_000_000, b)
}

// Many user acquires and releases 1M resources across 100 routines each
func BenchmarkManyUsersHighConcurrency(b *testing.B) {
	benchmarkThrottling(100, 100, 100_000_000, b)
}
