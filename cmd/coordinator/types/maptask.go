package types

import (
	"errors"
	"mapreduce/pkg/mr"
	"net/netip"
)

func NewMapTask(path string) MapTask {
	return MapTask{
		path:  mr.MapTaskFilePath(path),
		state: Unassigned,
		addr:  nil,
	}
}

// XXX:
type MapTask struct {
	// TODO: we are in the same filesystem at the moment
	path  mr.MapTaskFilePath
	state TaskState
	addr  *netip.AddrPort
}

// XXX:
func (mt *MapTask) Assign() error {
	if mt.state == Done {
		return errors.New("The task is completed.")
	} else if mt.state == Assigned {
		return errors.New("The task is already assigned.")
	}
	mt.state = Assigned
	mt.addr = nil
	return nil
}

// XXX:
func (mt *MapTask) Unassign() error {
	if mt.state == Done {
		return errors.New("The task is completed.")
	} else if mt.state == Unassigned {
		return errors.New("The task is already unassigned.")
	}
	mt.state = Unassigned
	mt.addr = nil
	return nil
}

// XXX:
func (mt *MapTask) Done(addr netip.AddrPort) error {
	if mt.state == Done {
		return errors.New("The task is completed.")
	}
	mt.state = Done
	mt.addr = &addr
	return nil
}
