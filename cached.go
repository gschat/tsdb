package tsdb

import "sync"

type _Cached struct {
	sync.RWMutex                // mxin rw locker
	indexer      map[uint64]int // ringbuffer indexer
	ringbuffer   []*DBValue     // ring buffer
	header       int            // ring header
	tail         int            // ring tail
}

func newCached(cachedsize int) *_Cached {
	return &_Cached{
		indexer:    make(map[uint64]int),
		ringbuffer: make([]*DBValue, cachedsize),
	}
}

func (cached *_Cached) Get(id uint64) (*DBValue, bool) {
	cached.RLock()
	defer cached.RUnlock()

	if indexer, ok := cached.indexer[id]; ok {
		return cached.ringbuffer[indexer], true
	}

	return nil, false
}

func (cached *_Cached) Update(val *DBValue) {
	cached.Lock()
	defer cached.Unlock()

	old := cached.ringbuffer[cached.tail]

	if old != nil {
		delete(cached.indexer, old.ID)
	}

	cached.ringbuffer[cached.tail] = val
	cached.indexer[val.ID] = cached.tail

	cached.tail++

	if cached.tail == len(cached.ringbuffer) {
		cached.tail = 0
	}

	if cached.tail == cached.header {
		cached.header++

		if cached.header == len(cached.ringbuffer) {
			cached.header = 0
		}
	}
}
