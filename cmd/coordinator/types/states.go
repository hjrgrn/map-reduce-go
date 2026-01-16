package types

type TaskState int

const (
	Unassigned TaskState = iota
	Assigned
	Done
)

type CoordinatorState int

const (
	Map CoordinatorState = iota
	Reduce
	Completed
)
