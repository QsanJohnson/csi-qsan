package driver

import (
	"fmt"
	"sync"
)

type ThresholdLimiter struct {
	mu       sync.Mutex
	inFlight int
	limit    int
}

// NewThresholdLimiter creates a limiter that allows up to `limit` concurrent operations.
func NewThresholdLimiter(limit int) *ThresholdLimiter {
	return &ThresholdLimiter{
		limit: limit,
	}
}

// TryEnter attempts to enter the critical section.
func (l *ThresholdLimiter) TryEnter() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.inFlight >= l.limit {
		return fmt.Errorf("current: %d, max: %d", l.inFlight, l.limit)
	}

	l.inFlight++
	return nil
}

// Leave should be called when the operation is finished to release the slot.
func (l *ThresholdLimiter) Leave() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.inFlight > 0 {
		l.inFlight--
	}
}
