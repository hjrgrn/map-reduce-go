package types

// Bucket represents a group of intermediate files produced by Map workers and consumed by Reduce workers.
// Each bucket is identified by an index, and each Reduce worker fetches all intermediate files
// with that index from every Map worker.
// The work is considered complete when all buckets reach the `Done` state.
type Bucket struct {
	// state indicates the current state of the bucket, either `Pending` or `Done`.
	state TaskState
	// id is the identifier for this bucket
	id int
}

// Returns a new instance of Bucket identified by `id`.
func NewBucket(id int) Bucket {
	return Bucket{id: id}
}

// Changes the state of `b` into `Done`.
func (b *Bucket) Done() {
	b.state = Done
}
