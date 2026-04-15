package types

//
// Subpackage that encloses the logic relative to map tasks.
//

import (
	"errors"
	"mapreduce/pkg/mr"
	"net/netip"
)

// Initiates a `MapTask` instace.
func NewMapTask(path string) MapTask {
	return MapTask{
		path:  mr.MapTaskFilePath(path),
		state: Pending,
		addr:  nil,
	}
}

// Represents a task that will be assigned to a Map Worker.
type MapTask struct {
	// Path of the file that will be parsed by a Map Worker
	// TODO: we are in the same filesystem at the moment
	path mr.MapTaskFilePath
	// State of the task. It can be `Assigned`, `Unassigned` or `Done`.
	state TaskState
	// IP address and port number of the Map Worker that is processing the file.
	// TODO: since we are always dealing with deterministic tasks, we may have multiple workers working
	// on  a single MapTask, and so, multiple workers containing the same intermediate files. This field
	// should probably be of type `[]*netip.AddrPort`.
	addr *netip.AddrPort
}

// Changes `state` of the `MapTask` to `Done` and `addr` to the address of the
// Map Worker that contains the intermediate files for this specific task.
// When all the `MapTask`s are in `Done` state Reduce Workers will start
// operate on the intermediate files produced by Map Workers
// If the task is already done it returns an error.
func (mt *MapTask) Done(addr netip.AddrPort) error {
	if mt.state == Done {
		return errors.New("The task is completed.")
	}
	mt.state = Done
	mt.addr = &addr
	return nil
}
