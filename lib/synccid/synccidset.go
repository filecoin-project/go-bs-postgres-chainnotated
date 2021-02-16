package synccid

import (
	"bytes"
	"sort"
	"sync"

	"github.com/ipfs/go-cid"
	pool "github.com/libp2p/go-buffer-pool"
)

// Set provides a kludge-y concurrency-safe version of cid.Set
//
// Just like the original it is an implementation of a set of Cids,
// which holds a single copy of every Cids that is added to it.
//
// Unlike the original there is no constructor: the zero value of a
// Set struct is ready to be used as a new empty Set
type Set struct {
	mu  sync.Mutex
	set map[string]struct{}
}

const (
	initSetSize = 64
)

// Has returns true if the given Cid is contained in the set
func (cs *Set) Has(c cid.Cid) (isContained bool) {
	cs.mu.Lock()

	if cs.set != nil {
		_, isContained = cs.set[c.KeyString()]
	}

	cs.mu.Unlock()
	return
}

// Add puts a Cid in the Set
func (cs *Set) Add(cids ...cid.Cid) {
	cs.mu.Lock()
	if cs.set == nil {
		cs.set = make(map[string]struct{}, initSetSize)
	}
	for i := range cids {
		// do not record the empty cid
		if cids[i].KeyString() != "" {
			cs.set[cids[i].KeyString()] = struct{}{}
		}
	}
	cs.mu.Unlock()
}

// Visit inserts a Cid into the set and returns true if
// the result was a new addition or false if the set
// already contained the given Cid
func (cs *Set) Visit(c cid.Cid) (isNewMember bool) {
	// do not record the empty cid
	if c.KeyString() == "" {
		return false
	}

	cs.mu.Lock()

	if cs.set == nil {
		cs.set = make(map[string]struct{}, initSetSize)
	}
	if _, found := cs.set[c.KeyString()]; !found {
		cs.set[c.KeyString()] = struct{}{}
		isNewMember = true
	}

	cs.mu.Unlock()
	return
}

// Remove deletes a Cid from the Set
func (cs *Set) Remove(cids ...cid.Cid) {
	cs.mu.Lock()

	if cs.set != nil {
		for i := range cids {
			delete(cs.set, cids[i].KeyString())
		}
	}

	cs.mu.Unlock()
}

// RemoveKeyString deletes a Cid by its KeyString
func (cs *Set) RemoveKeyString(cidKeyStrings ...string) {
	cs.mu.Lock()

	if cs.set != nil {
		for i := range cidKeyStrings {
			delete(cs.set, cidKeyStrings[i])
		}
	}

	cs.mu.Unlock()
}

// Len returns how many elements the Set has
func (cs *Set) Len() int {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.set == nil {
		return 0
	}
	return len(cs.set)
}

// ForEach allows to run a custom function on each
// Cid in the set
func (cs *Set) ForEach(f func(cid.Cid) error) (err error) {
	allCids := cs.Keys()
	for _, c := range allCids {
		if err = f(c); err != nil {
			return
		}
	}
	return
}

// Keys returns the Cids in the set
func (cs *Set) Keys() []cid.Cid {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.set == nil {
		return make([]cid.Cid, 0)
	}

	ret := make([]cid.Cid, 0, len(cs.set))
	for cidKeyString := range cs.set {

		cidBytes := pool.Get(len(cidKeyString))
		copy(cidBytes, cidKeyString)
		_, c, _ := cid.CidFromBytes(cidBytes) // no error checks: we Add() only cid.Cid's - this can't be invalid
		pool.Put(cidBytes)

		ret = append(ret, c)
	}

	return ret
}

// SortedRawKeys returns the sorted list of cidmember.Keystring()
// of all Cids in the set
func (cs *Set) SortedRawKeys() [][]byte {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.set == nil {
		return make([][]byte, 0)
	}

	ret := make([][]byte, 0, len(cs.set))
	for cidKeyString := range cs.set {
		ret = append(ret, []byte(cidKeyString))
	}

	// might as well, it doesn't cost anything in practice
	sort.Slice(ret, func(i, j int) bool {
		return bytes.Compare(ret[i], ret[j]) < 0
	})

	return ret
}

// Clone allocates a new independent set and populates it with the
// contents of the cloned set
func (cs *Set) Clone() *Set {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.set == nil {
		return &Set{}
	}

	newSet := &Set{
		set: make(map[string]struct{}, len(cs.set)),
	}
	for k := range cs.set {
		newSet.set[k] = struct{}{}
	}

	return newSet
}
