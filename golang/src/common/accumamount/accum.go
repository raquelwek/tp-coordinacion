package sum

import (
	"sync"
)

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
	WaitFor(clientID string, target uint64) <-chan struct{}

	// In case the EOF has arrived and the main thread is waiting
	// to reach de target ammount it closes the channel to notify
	// it has been reached.
	CheckAndNotify(clientID string)

	// Adds to the `client_id“ accumulator the quantity
	// indicated by `num`
	Add(client_id string, num uint64)

	// Returns true if there are no more waiters for the client id
	// which is true only if the channel is closed.
	ChannelHasBeenClosed(clientId string, target uint64) bool
}

func NewAccumAmount() AccumAmount {
	return &accumAmount{accumulator: make(map[string]uint64), waiters: make(map[string]waiter)}
}
func (accumAmount *accumAmount) WaitFor(clientId string, target uint64) <-chan struct{} {
	accumAmount.mu.Lock()
	defer accumAmount.mu.Unlock()

	ch := make(chan struct{})
	if accumAmount.accumulator[clientId] == target {
		close(ch)
		return ch
	}
	accumAmount.waiters[clientId] = waiter{ch: ch, target: target}
	return ch
}

func (accumAmount *accumAmount) CheckAndNotify(clientId string) {
	accumAmount.mu.Lock()
	defer accumAmount.mu.Unlock()

	w, ok := accumAmount.waiters[clientId]
	if ok && accumAmount.accumulator[clientId] == w.target {
		close(w.ch)
		delete(accumAmount.waiters, clientId)
	}
}

func (accumAmount *accumAmount) Add(client_id string, num uint64) {
	accumAmount.mu.Lock()
	defer accumAmount.mu.Unlock()
	_, ok := accumAmount.accumulator[client_id]
	if ok {
		accumAmount.accumulator[client_id] += num
	} else {
		accumAmount.accumulator[client_id] = num
	}
}

func (accumAmount *accumAmount) ChannelHasBeenClosed(clientId string, target uint64) bool {
	accumAmount.mu.Lock()
	defer accumAmount.mu.Unlock()
	_, ok := accumAmount.waiters[clientId]
	return !ok && accumAmount.accumulator[clientId] == target

}
