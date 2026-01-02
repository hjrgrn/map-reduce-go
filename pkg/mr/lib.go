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
// GetMapTask RPC Arguments
//

type GetMapTaskArgs struct{}

type GetMapTaskReply struct {
	Path           MapTaskFilePath
	MapIsCompleted bool
	Buckets        int
}

//
// GetMapTask RPC Arguments
//

type MapCompletedArgs struct {
	Path MapTaskFilePath
	Addr netip.AddrPort
}

type MapCompletedReply struct{}

// XXX:
type MapTaskFilePath string
