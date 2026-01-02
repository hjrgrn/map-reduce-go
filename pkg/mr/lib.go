package mr

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
}

//
// GetMapTask RPC Arguments
//

type MapCompletedArgs struct {
	Path MapTaskFilePath
}

type MapCompletedReply struct{}

// XXX:
type MapTaskFilePath string
