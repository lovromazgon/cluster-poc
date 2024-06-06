package cluster

import (
	"fmt"
	"io"
	"strconv"

	"github.com/hashicorp/raft"
)

type FSM struct {
	sum int
}

// Apply is called once a log entry is committed by a majority of the cluster.
//
// Apply should apply the log to the FSM. Apply must be deterministic and
// produce the same result on all peers in the cluster.
//
// The returned value is returned to the client as the ApplyFuture.Response.
func (f *FSM) Apply(l *raft.Log) interface{} {
	num, _ := strconv.Atoi(string(l.Data))
	f.sum += num
	if num < 0 {
		fmt.Printf("APPLY: %d-%d, SUM: %d\n", f.sum-num, -num, f.sum)
	} else {
		fmt.Printf("APPLY: %d+%d, SUM: %d\n", f.sum-num, num, f.sum)
	}
	return f.sum
}

// Snapshot returns an FSMSnapshot used to: support log compaction, to
// restore the FSM to a previous state, or to bring out-of-date followers up
// to a recent log index.
//
// The Snapshot implementation should return quickly, because Apply can not
// be called while Snapshot is running. Generally this means Snapshot should
// only capture a pointer to the state, and any expensive IO should happen
// as part of FSMSnapshot.Persist.
//
// Apply and Snapshot are always called from the same thread, but Apply will
// be called concurrently with FSMSnapshot.Persist. This means the FSM should
// be implemented to allow for concurrent updates while a snapshot is happening.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return snapshot{sum: f.sum}, nil
}

// Restore is used to restore an FSM from a snapshot. It is not called
// concurrently with any other command. The FSM must discard all previous
// state before restoring the snapshot.
func (f *FSM) Restore(read io.ReadCloser) error {
	defer read.Close()
	b, err := io.ReadAll(read)
	if err != nil {
		return err
	}
	sum, err := strconv.Atoi(string(b))
	if err != nil {
		return err
	}
	f.sum = sum
	return nil
}

var _ raft.FSM = &FSM{}

type snapshot struct {
	sum int
}

// Persist should dump all necessary state to the WriteCloser 'sink',
// and call sink.Close() when finished or call sink.Cancel() on error.
func (s snapshot) Persist(sink raft.SnapshotSink) error {
	_, err := sink.Write([]byte(strconv.Itoa(s.sum)))
	return err
}

// Release is invoked when we are finished with the snapshot.
func (s snapshot) Release() {}
