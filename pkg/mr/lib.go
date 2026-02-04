package mr

import "net/netip"

//
// RPC definitions.
//

//
// Example RPC arguments
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

//
// GetTask RPC Arguments
//

type GetTaskArgs struct{}

// TODO: maybe use oneof
// TODO: enforce exclusive or
type GetTaskReply struct {
	MapReply    *GetMapTaskReply
	ReduceReply *GetReduceTaskReply
}

// XXX:
type GetMapTaskReply struct {
	// Path to the file that has been assigned to the requiring Map Worker.
	Path MapTaskFilePath
	// The amount of intermediate files that will be produced.
	Buckets int
	// XXX:
	Index int
	// XXX:
	MapIsCompleted bool
}

// XXX:
type GetReduceTaskReply struct {
	// XXX:
	Addresses []*netip.AddrPort
	// XXX:
	Bucket int
}

//
// MapCompleted RPC Arguments
//

type MapCompletedArgs struct {
	// XXX:
	Index int
	// Address and port number of the Map Worker that has completed the task.
	Addr netip.AddrPort
}

type MapCompletedReply struct{}

type MapTaskFilePath string

type IntermediateFilePath string
