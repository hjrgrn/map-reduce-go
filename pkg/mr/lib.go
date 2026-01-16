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
// TODO: enforce exclusive or nothing for done
type GetTaskReply struct {
	MapReply *GetMapTaskReply
	ReduceReply *GetReduceTaskReply
}

// XXX:
type GetMapTaskReply struct {
	// Path to the file that has been assigned to the requiring Map Worker.
	Path MapTaskFilePath
	// The amount of intermediate files that will be produced.
	Buckets int
}

// XXX:
type GetReduceTaskReply struct {
	// TODO:
}

//
// MapCompleted RPC Arguments
//

type MapCompletedArgs struct {
	// The Path of the file that has been parsed.
	// At this point the path of the file won't be used anymore, since the
	// intermediate files that the Reduce Workers will use are on the filesystems
	// of the Map Workers, but the path identifies the Map Task for Coordinator.
	Path MapTaskFilePath
	// Address and port number of the Map Worker that has completed the task.
	Addr netip.AddrPort
}

type MapCompletedReply struct{}

type MapTaskFilePath string

type IntermediateFilePath string
