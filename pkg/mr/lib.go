package mr

import (
	"mapreduce/pkg/utils"
	"net/netip"
	"time"
)

//
// RPC definitions.
//

//
// CheckHealth RPC arguments
//

type CheckHealthArgs struct{}

type CheckHealthReply struct {
	State  utils.State
	Uptime time.Duration
}

type GetMapTaskArgs struct{}

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

type GetReduceTaskArgs struct{}

// XXX:
type GetReduceTaskReply struct {
	// XXX:
	Addresses []*netip.AddrPort
	// XXX:
	Bucket int
	// XXX
	ReduceIsCompleted bool
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

type MapCompletedReply struct {
	Failure bool
}

//
// ReduceCompleted RPC Arguments
//

type ReduceCompletedArgs struct {
	// XXX:
	Bucket int
}

type ReduceCompletedReply struct {
	Failure bool
}

type MapTaskFilePath string

type IntermediateFilePath string
