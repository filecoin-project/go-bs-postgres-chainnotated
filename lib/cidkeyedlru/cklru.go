package cidkeyedlru

import (
	"container/list"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/dustin/go-humanize"
	"github.com/ipfs/go-cid"
)

// CidKeyedLRU implements a cost-keeping LRU cache keyed by Cid.
//
// The cache is implemented as a classic double-linked-list combined with a
// map-based lookup-dictionary, except that in order to alleviate allocation
// coarseness there are 256 dictionaries: one for each tail-byte of a Cid. The
// defaults cache is pre-sized such that its internal structures occupy about
// 1GiB of memory.
//
// Every addition must be accompanied with an integer cost (what does the cost
// represent is up to the user). Evictions of least-recently-used items take
// place once an addition causes the total cost to increase beyond the
// configured threshold. The size of internal structures is *NOT* considered:
// the user must take these into account.
type CidKeyedLRU interface {
	Get(c cid.Cid) (object interface{}, found bool)
	Peek(c cid.Cid) (object interface{}, found bool)
	Populate([]*LruEntry)
	Add(c cid.Cid, object interface{}, cost int64) (wasNewlyAdded bool)
	Evict(c cid.Cid) (foundAndEvicted bool)
	Purge()
	Metrics() Metrics
	StatString() string
}

// LruEntry is a container representing a single cache entry "unit"
type LruEntry struct {
	Cost   int64
	Cid    cid.Cid
	Object interface{}
}

// Metrics is a set of simple counters reflecting the cache state.
type Metrics struct {
	GetHits      int64
	GetMisses    int64
	CountAdded   int64
	CostAdded    int64
	CountEvicted int64
	CostEvicted  int64
}

// above GetHits/GetMisses are manipulated without locks, via atomic.AddInt64

const (
	minMaxCost      = 1024
	initSubdictSize = 1 << 16 // about 4MiB/map, 1GiB total
)

// NewCidKeyedLruCache initializes a new CidKeyedLRU cache with the given
// maximum cost.
func NewCidKeyedLruCache(maxCost int64) (*CKLru, error) {
	if maxCost < minMaxCost {
		return nil, fmt.Errorf("maxCost can not be smaller than %d", minMaxCost)
	}

	l := CKLru{
		maxCost: maxCost,
		ordered: list.New(),
	}
	for i := range l.lookups {
		l.lookups[i] = make(map[cid.Cid]*list.Element, initSubdictSize)
	}

	return &l, nil
}

type CKLru struct {
	maxCost   int64
	metrics   Metrics
	lookupsMu sync.RWMutex                   // always locked first
	lookups   [256]map[cid.Cid]*list.Element // using one huge hash is too coarse RAM-wise
	orderedMu sync.Mutex                     // this is locked second, only when changing the linked-list
	ordered   *list.List
}

func tailByte(c cid.Cid) byte {
	k := c.KeyString()
	if len(k) == 0 {
		return 0
	}
	return k[len(k)-1]
}

// Get attempts to retrieve an object from the cache, and returns a boolean
// indicating whether an entry was found. The retrieved item is bumped to
// the front of the LRU queue.
func (l *CKLru) Get(c cid.Cid) (interface{}, bool) {
	l.lookupsMu.RLock()
	defer l.lookupsMu.RUnlock()

	elt, found := l.lookups[tailByte(c)][c]
	if !found {
		atomic.AddInt64(&l.metrics.GetMisses, 1)
		return nil, false
	}

	atomic.AddInt64(&l.metrics.GetHits, 1)

	l.orderedMu.Lock()
	l.ordered.MoveToFront(elt)
	l.orderedMu.Unlock()

	return elt.Value.(*LruEntry).Object, true
}

// Peek is identical to Get, except that no change in metrics takes place and
// a "refresh" is not triggered: the object remains exactly where it was in
// the eviction queue.
func (l *CKLru) Peek(c cid.Cid) (interface{}, bool) {
	l.lookupsMu.RLock()
	defer l.lookupsMu.RUnlock()

	elt, found := l.lookups[tailByte(c)][c]
	if !found {
		return nil, false
	}

	return elt.Value.(*LruEntry).Object, true
}

// Add attempts to insert the given object keyed by the given cid, and returns
// a boolean indicating whether the insertion took place. If an entry already
// exists under the given key the insertion DOES NOT take place: in order to
// replace an entry you first must Evict() the existing one.
//
// In order to reduce LRU instability, newly added objects are inserted randomly
// into the current LRU map. If you want to ensure an object stays at the front
// of the queue - you need to Get() it almost immediately after the Add().
func (l *CKLru) Add(c cid.Cid, object interface{}, cost int64) bool {

	// lightweight check - if it is there we don't have to block Get()s
	l.lookupsMu.RLock()
	_, found := l.lookups[tailByte(c)][c]
	l.lookupsMu.RUnlock()
	if found {
		return false
	}

	if cost <= 0 {
		panic("Add() with negative or zero cost unsupported")
	}

	l.lookupsMu.Lock()
	l.orderedMu.Lock()
	defer func() { l.orderedMu.Unlock(); l.lookupsMu.Unlock() }()

	subDict := l.lookups[tailByte(c)]

	// perform the check once more with all locks now in place
	// contention here can at times be very severe
	if _, found := subDict[c]; found {
		return false
	}

	for l.ordered.Len() > 0 && l.metrics.CostAdded-l.metrics.CostEvicted+cost >= l.maxCost {
		shifted := l.ordered.Remove(l.ordered.Back()).(*LruEntry)
		delete(l.lookups[tailByte(shifted.Cid)], shifted.Cid)
		l.metrics.CostEvicted += shifted.Cost
		l.metrics.CountEvicted++
	}

	newEntry := &LruEntry{
		Cost:   cost,
		Cid:    c,
		Object: object,
	}

	if len(subDict) == 0 {
		subDict[c] = l.ordered.PushFront(newEntry)
	} else {
		// Get a pseudo-random item and add our entry in front of it
		// This is a cheap way to avoid an "eviction-storm" after a series of mass-Add()s
		var randomElt *list.Element
		for _, randomElt = range subDict {
			break
		}
		subDict[c] = l.ordered.InsertBefore(newEntry, randomElt) // InsertBefore() == closer-to-Front()
	}

	l.metrics.CostAdded += cost
	l.metrics.CountAdded++

	return true
}

// Populate inserts multiple entries into the cache in one go, significantly
// reducing lock contention. Insertion rules are identical to Add() - if an
// entry already exists it is skipped, and its "freshness" is not adjusted.
// In order to replace all entries, you must first Purge() the cache.
func (l *CKLru) Populate(entries []*LruEntry) {

	if len(entries) == 0 {
		return
	} else if len(entries) == 1 {
		l.Add(entries[0].Cid, entries[0].Object, entries[0].Cost)
		return
	}

	l.lookupsMu.RLock()

	var newCost int64
	var toInsertGroups [256][]*LruEntry
	for i := range entries {
		tb := tailByte(entries[i].Cid)

		if _, found := l.lookups[tb][entries[i].Cid]; found {
			continue
		}

		if toInsertGroups[tb] == nil {
			toInsertGroups[tb] = make([]*LruEntry, 0, 4096)
		}
		toInsertGroups[tb] = append(toInsertGroups[tb], entries[i])

		if entries[i].Cost <= 0 {
			l.lookupsMu.RUnlock()
			panic("Populate() with negative or zero cost unsupported")
		}
		newCost += entries[i].Cost
	}

	l.lookupsMu.RUnlock()

	if newCost == 0 {
		return
	}

	l.lookupsMu.Lock()
	l.orderedMu.Lock()
	defer func() { l.orderedMu.Unlock(); l.lookupsMu.Unlock() }()

	if newCost >= l.maxCost {
		// ugh... we will overflow the cache... oh well... just purge
		l.ordered.Init()
		for i := range l.lookups {
			l.lookups[i] = make(map[cid.Cid]*list.Element, initSubdictSize)
		}
		l.metrics = Metrics{}
	} else {
		for l.ordered.Len() > 0 && l.metrics.CostAdded-l.metrics.CostEvicted+newCost >= l.maxCost {
			shifted := l.ordered.Remove(l.ordered.Back()).(*LruEntry)
			delete(l.lookups[tailByte(shifted.Cid)], shifted.Cid)
			l.metrics.CostEvicted += shifted.Cost
			l.metrics.CountEvicted++
		}
	}

	spaceLeft := l.maxCost - (l.metrics.CostAdded - l.metrics.CostEvicted)

	for tb := range toInsertGroups {
		if toInsertGroups[tb] == nil {
			continue
		}

		subDict := l.lookups[tb]
		subGroup := toInsertGroups[tb]

		for _, entry := range subGroup {

			// perform the check once more with all locks now in place
			// contention here can at times be very severe
			if _, found := subDict[entry.Cid]; found {
				continue
			}

			// don't allow an overflow
			if spaceLeft-entry.Cost < 0 {
				continue // a subsequent entry might be small enough to fit
			}
			spaceLeft -= entry.Cost

			if len(subDict) == 0 {
				subDict[entry.Cid] = l.ordered.PushFront(entry)
			} else {
				// Get a pseudo-random item and add our entry in front of it
				// This is a cheap way to avoid an "eviction-storm" right after
				var randomElt *list.Element
				for _, randomElt = range subDict {
					break
				}
				subDict[entry.Cid] = l.ordered.InsertBefore(entry, randomElt) // InsertBefore() == closer-to-Front()
			}

			l.metrics.CostAdded += entry.Cost
			l.metrics.CountAdded++
		}
	}
}

// Evict attempts to remove the object keyed by the given Cid, and returns true
// if such an object was found and removed.
func (l *CKLru) Evict(c cid.Cid) bool {
	l.lookupsMu.RLock()
	_, found := l.lookups[tailByte(c)][c]
	l.lookupsMu.RUnlock()
	if !found {
		return false
	}

	l.lookupsMu.Lock()
	l.orderedMu.Lock()

	// perform the check once more with all locks now in place
	// contention here can at times be very severe
	elt, found := l.lookups[tailByte(c)][c]
	if !found {
		return false
	}

	delete(l.lookups[tailByte(c)], c)

	l.metrics.CostEvicted += l.ordered.Remove(elt).(*LruEntry).Cost
	l.metrics.CountEvicted++

	l.orderedMu.Unlock()
	l.lookupsMu.Unlock()

	return true
}

// Purge empties the cache entirely.
func (l *CKLru) Purge() {
	l.lookupsMu.Lock()
	l.orderedMu.Lock()

	l.ordered.Init()
	l.metrics = Metrics{}
	for i := range l.lookups {
		l.lookups[i] = make(map[cid.Cid]*list.Element, initSubdictSize)
	}

	l.orderedMu.Unlock()
	l.lookupsMu.Unlock()
}

// Reallocate is a TEMPORARY tool to figure out where memory is going.
func (l *CKLru) Reallocate() {
	l.lookupsMu.Lock()

	for i := range l.lookups {
		newL := make(map[cid.Cid]*list.Element, len(l.lookups[i]))
		for k, v := range l.lookups[i] {
			newL[k] = v
		}
		l.lookups[i] = newL
	}

	l.lookupsMu.Unlock()
}

// Metrics returns a copy of the current Metrics structure.
func (l *CKLru) Metrics() (metricsCopy Metrics) {
	l.lookupsMu.Lock()
	metricsCopy = l.metrics

	// FIXME - silly sanity check, remove at some point
	{
		l.orderedMu.Lock()
		var allSubdictsElementCount int
		for _, m := range l.lookups {
			allSubdictsElementCount += len(m)
		}
		if allSubdictsElementCount != l.ordered.Len() {
			l.orderedMu.Unlock()
			panic(fmt.Sprintf(
				"impossible(?) mismatch of element-count in the lookup dictionaries (%d) and linked-list (%d)",
				allSubdictsElementCount,
				l.ordered.Len(),
			))
		}
		l.orderedMu.Unlock()
	}

	l.lookupsMu.Unlock()
	return
}

// StatString calls Metrics() and turns the contents into a summary string
// suitable for log/debug output
func (l *CKLru) StatString() string {
	m := l.Metrics()

	var pctFull, pctHit float64
	curCost := m.CostAdded - m.CostEvicted
	if curCost != 0 {
		pctFull = float64(curCost) * 100 / float64(l.maxCost)
		pctHit = float64(m.GetHits) * 100 / float64(m.GetHits+m.GetMisses)
	}

	return fmt.Sprintf(
		`--- BLOCK CACHE STATS
---
Current Entries: % 12s
Current Cost: % 14s  (%0.2f%% full)
Hit Ratio: %.02f%%
Hits:% 12s   Misses:% 12s
Adds:% 12s  Evicted:% 12s`,
		humanize.Comma(m.CountAdded-m.CountEvicted),
		humanize.Comma(curCost), pctFull,
		pctHit,
		humanize.Comma(m.GetHits), humanize.Comma(m.GetMisses),
		humanize.Comma(m.CountAdded), humanize.Comma(m.CountEvicted),
	)
}
