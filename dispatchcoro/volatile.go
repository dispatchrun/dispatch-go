package dispatchcoro

import (
	"fmt"
	"math/rand/v2"
	"sync"
)

// VolatileCoroutines is a set of volatile coroutine instances.
//
// "Instances" are only applicable when coroutines are running
// in volatile mode, since suspended coroutines must be kept in
// memory. In durable mode, there's no need to keep instances
// around, since they can be serialized and later recreated.
type VolatileCoroutines struct {
	instances map[InstanceID]Coroutine
	nextID    InstanceID
	mu        sync.Mutex
}

// InstanceID is a unique identifier for a coroutine instance.
type InstanceID = uint64

// Register registers a coroutine instance and returns a unique
// identifier.
func (f *VolatileCoroutines) Register(coro Coroutine) InstanceID {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.nextID == 0 {
		f.nextID = rand.Uint64()
	}
	f.nextID++

	id := f.nextID
	if f.instances == nil {
		f.instances = map[InstanceID]Coroutine{}
	}
	f.instances[id] = coro

	return id
}

// Find finds the coroutine instance with the specified ID.
func (f *VolatileCoroutines) Find(id InstanceID) (Coroutine, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	coro, ok := f.instances[id]
	if !ok {
		return coro, fmt.Errorf("volatile coroutine %d not found", id)
	}
	return coro, nil
}

// Delete deletes a coroutine instance.
func (f *VolatileCoroutines) Delete(id InstanceID) {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.instances, id)
}

// Close closes the set of coroutine instances.
func (f *VolatileCoroutines) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, fn := range f.instances {
		fn.Stop()
		fn.Next()
	}
	clear(f.instances)
	return nil
}
