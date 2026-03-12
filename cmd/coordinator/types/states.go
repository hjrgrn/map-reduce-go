package types

type TaskState int

const (
	Pending TaskState = iota
	Done
)

type CoordinatorState int

const (
	Map CoordinatorState = iota
	Reduce
	Completed
)
