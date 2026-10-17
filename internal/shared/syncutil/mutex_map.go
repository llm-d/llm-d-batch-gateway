package syncutil

import "sync"

// MutexMap is a type-safe concurrent map guarded by a sync.Mutex.
type MutexMap[K comparable, V any] struct {
	mu sync.Mutex
	m  map[K]V
}

func NewMutexMap[K comparable, V any]() *MutexMap[K, V] {
	return &MutexMap[K, V]{m: make(map[K]V)}
}

func (mm *MutexMap[K, V]) Store(key K, val V) {
	mm.mu.Lock()
	mm.m[key] = val
	mm.mu.Unlock()
}

func (mm *MutexMap[K, V]) Load(key K) (V, bool) {
	mm.mu.Lock()
	val, ok := mm.m[key]
	mm.mu.Unlock()
	return val, ok
}

// LoadOrStore returns the existing value for the key if present.
// Otherwise, it stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
func (mm *MutexMap[K, V]) LoadOrStore(key K, val V) (actual V, loaded bool) {
	mm.mu.Lock()
	existing, ok := mm.m[key]
	if ok {
		mm.mu.Unlock()
		return existing, true
	}
	mm.m[key] = val
	mm.mu.Unlock()
	return val, false
}

func (mm *MutexMap[K, V]) LoadAndDelete(key K) (V, bool) {
	mm.mu.Lock()
	val, ok := mm.m[key]
	if ok {
		delete(mm.m, key)
	}
	mm.mu.Unlock()
	return val, ok
}

func (mm *MutexMap[K, V]) Delete(key K) {
	mm.mu.Lock()
	delete(mm.m, key)
	mm.mu.Unlock()
}

// Range calls fn for each key-value pair, in unspecified order, stopping
// early when fn returns false. fn runs while the map's mutex is held: it
// must not block and must not call back into the same MutexMap.
func (mm *MutexMap[K, V]) Range(fn func(K, V) bool) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	for k, v := range mm.m {
		if !fn(k, v) {
			return
		}
	}
}

func (mm *MutexMap[K, V]) Keys() []K {
	mm.mu.Lock()
	keys := make([]K, 0, len(mm.m))
	for k := range mm.m {
		keys = append(keys, k)
	}
	mm.mu.Unlock()
	return keys
}

func (mm *MutexMap[K, V]) Len() int {
	mm.mu.Lock()
	n := len(mm.m)
	mm.mu.Unlock()
	return n
}
