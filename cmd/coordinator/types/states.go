package types

type TaskState int

const (
	TaskPending TaskState = iota
	TaskDone
)

type CoordinatorState int

const (
	Map CoordinatorState = iota
	Reduce
	Completed
)
