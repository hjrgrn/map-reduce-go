package types

type TaskState int

const (
	Unassigned TaskState = iota
	Assigned
	Done
)
