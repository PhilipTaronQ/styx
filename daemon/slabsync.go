package daemon

import (
	"fmt"
	"sync"

	"golang.org/x/sys/unix"
)

// slabSyncer shares fdatasync calls on one slab among concurrent callers.
type slabSyncer struct {
	mu      sync.Mutex
	cond    sync.Cond
	started uint64 // syncs started
	done    uint64 // syncs finished
	running bool
	err     error // result of the last finished sync
}

// syncSlab makes the writes to a slab that finished before the call durable. Chunks must be
// durable before the db records them present: bbolt syncs its commits, and a present key
// for data lost in a crash would make us read zeros for the chunk forever.
func (s *Server) syncSlab(slabId uint16) error {
	s.slabSyncLock.Lock()
	if s.slabSyncers == nil {
		s.slabSyncers = make(map[uint16]*slabSyncer)
	}
	ss := s.slabSyncers[slabId]
	if ss == nil {
		ss = &slabSyncer{}
		ss.cond.L = &ss.mu
		s.slabSyncers[slabId] = ss
	}
	s.slabSyncLock.Unlock()

	ss.mu.Lock()
	defer ss.mu.Unlock()
	// only a sync that starts after now covers our writes
	want := ss.started + 1
	for ss.done < want {
		if ss.running {
			ss.cond.Wait()
			continue
		}
		ss.running = true
		ss.started++
		ss.mu.Unlock()
		err := s.fdatasyncSlab(slabId)
		ss.mu.Lock()
		ss.running = false
		ss.done = ss.started
		ss.err = err
		ss.cond.Broadcast()
	}
	return ss.err
}

func (s *Server) fdatasyncSlab(slabId uint16) error {
	fd, err := s.dupCacheFd(slabId)
	if err != nil {
		// the slab image isn't mounted yet; its object's write fd reaches the same file
		if fd, err = s.dupWriteFdForSlab(slabId); err != nil {
			return fmt.Errorf("no fd to sync slab %d: %w", slabId, err)
		}
	}
	defer unix.Close(fd)
	return unix.Fdatasync(fd)
}
