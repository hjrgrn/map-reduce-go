package main

//
// Start the coordinator process
//
// Usage:
//
// ```bash
// go run ./cmd/coordinator/main.go \
//  ./testdata/pg-sherlock_holmes.txt \
// 	./testdata/pg-grim.txt \
//  ./testdata/frankenstein.txt
// ```

import (
	"errors"
	"fmt"
	"log"
	"mapreduce/pkg/mr"
	"net"
	"net/http"
	"net/netip"
	"net/rpc"
	"os"
	"sync"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: coordinator inputfiles...\n")
		os.Exit(1)
	}

	m := MakeCoordinator(os.Args[1:], 10)
	for m.done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}

type Coordinator struct {
	mutex     sync.Mutex
	map_tasks map[mr.MapTaskFilePath]MapTask
}

// XXX:
func build_coordinator(files []string) Coordinator {
	tasks := make(map[mr.MapTaskFilePath]MapTask, len(files))
	for i := range files {
		task := NewMapTask(files[i])
		tasks[task.path] = task
	}

	return Coordinator{map_tasks: tasks}
}

// main calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) done() bool {
	ret := false

	// XXX:

	return ret
}

// an example RPC handler.
func (c *Coordinator) Example(args *mr.ExampleArgs, reply *mr.ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// XXX:
func (c *Coordinator) GetMapTask(args *mr.GetMapTaskArgs, reply *mr.GetMapTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	assigned := false
	for k, v := range c.map_tasks {
		if v.state == Unassigned {
			reply.Path = k
			assigned = true
			v.Assign(&args.Addr)
			// TODO: avoid copy
			c.map_tasks[k] = v
			reply.MapIsCompleted = false
			break
		}
	}
	if !assigned {
		reply.MapIsCompleted = true
	}

	return nil
}

// XXX:
type MapTask struct {
	// TODO: we are in the same filesystem at the moment
	path  mr.MapTaskFilePath
	state TaskState
	addr  *netip.Addr
}

// XXX:
func (mt *MapTask) Assign(addr *netip.Addr) error {
	if mt.state == Done {
		return errors.New("The task is completed.")
	} else if mt.state == Assigned {
		return errors.New("The task is already assigned.")
	}
	mt.state = Assigned
	mt.addr = addr
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
func (mt *MapTask) Done() error {
	if mt.state == Done {
		return errors.New("The task is completed.")
	}
	mt.state = Done
	mt.addr = nil
	return nil
}

// XXX:
type TaskState int

const (
	Unassigned TaskState = iota
	Assigned
	Done
)

func NewMapTask(path string) MapTask {
	return MapTask{
		path:  mr.MapTaskFilePath(path),
		state: Unassigned,
		addr:  nil,
	}
}

// Create a Coordinator.
// main calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// XXX:

	c.server()
	return &c
}

// Start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":5000")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}
