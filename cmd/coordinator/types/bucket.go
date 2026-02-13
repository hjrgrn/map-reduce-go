package types

// XXX:
type Bucket struct {
	state TaskState
	index int
}

func NewBucket(index int) Bucket {
	return Bucket{index: index}
}
