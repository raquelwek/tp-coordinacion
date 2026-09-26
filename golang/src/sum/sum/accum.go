package sum

import "sync"

type waiter struct {
	ch     chan struct{}
	target uint64
}

type accumAmount struct {
	accumulator map[string]uint64
	waiters     map[string]waiter
	mu          sync.Mutex
}

type AccumAmount interface {
	// Returns the channel to be close when the target
	// ammount is reached.
	waitFor(clientID string, target uint64) <-chan struct{}

	// In case the EOF has arrived and the main thread is waiting
	// to reach de target ammount it closes the channel to notify
	// it has been reached.
	checkAndNotify(clientID string)

	// Adds to the `client_id“ accumulator the quantity
	// indicated by `num`
	Add(client_id string, num uint64)
}

func NewAccumAmount() AccumAmount {
	return &accumAmount{accumulator: make(map[string]uint64), waiters: make(map[string]waiter)}
}
func (accumAmount *accumAmount) waitFor(clientID string, target uint64) <-chan struct{} {
	accumAmount.mu.Lock()
	defer accumAmount.mu.Unlock()

	ch := make(chan struct{})
	if accumAmount.accumulator[clientID] >= target {
		close(ch)
		return ch
	}
	accumAmount.waiters[clientID] = waiter{ch: ch, target: target}
	return ch
}

func (accumAmount *accumAmount) checkAndNotify(clientID string) {
	accumAmount.mu.Lock()
	defer accumAmount.mu.Unlock()

	w, ok := accumAmount.waiters[clientID]
	if ok && accumAmount.accumulator[clientID] >= w.target {
		close(w.ch)
		delete(accumAmount.waiters, clientID)
	}
}

func (accumAmount *accumAmount) Add(client_id string, num uint64) {
	accumAmount.mu.Lock()
	_, ok := accumAmount.accumulator[client_id]
	if ok {
		accumAmount.accumulator[client_id] += num
	} else {
		accumAmount.accumulator[client_id] = num
	}
	accumAmount.mu.Unlock()
}
