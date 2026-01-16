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
		state: TaskPending,
		addr:  nil,
	}
}

// Represents a task that will be assigned to a Map Worker.
type MapTask struct {
	// Path of the file that will be parsed by a Map Worker
	// TODO: we are in the same filesystem at the moment
	// TODO: this field is redundant, take a look at `Coordinator.map_tasks`
	path mr.MapTaskFilePath
	// State of the task. It can be `Assigned`, `Unassigned` or `Done`.
	state TaskState
	// IP address and port number of the Map Worker that is processing the file.
	addr *netip.AddrPort
}

// Changes `state` of the `MapTask` to `Done` and `addr` to the address of the
// Map Worker that contains the intermediate files for this specific task.
// When all the `MapTask`s are in `Done` state Reduce Workers will start
// operate on the intermediate files produced by Map Workers
// If the task is already done it returns an error.
func (mt *MapTask) Done(addr netip.AddrPort) error {
	if mt.state == TaskDone {
		return errors.New("The task is completed.")
	}
	mt.state = TaskDone
	mt.addr = &addr
	return nil
}
