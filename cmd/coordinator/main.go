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
	"fmt"
	"log"
	"mapreduce/pkg/mr"
	"net"
	"net/http"
	"net/rpc"
	"os"
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
	MapTasks []MapTask
}

// XXX:
func build_coordinator(files []string) Coordinator {
	tasks := make([]MapTask, len(files))
	for i := range files {
		task := NewMapTask(files[i])
		tasks = append(tasks, task)
	}

	return Coordinator{MapTasks: tasks}
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
type MapTask struct {
	// TODO: we are in the same filesystem at the moment
	path  MapTaskFilePath
	state TaskState
}

func (mt *MapTask) NextStage() {
	if mt.state == Unassigned {
		mt.state = Assigned
	} else if mt.state == Assigned {
		mt.state = Done
	}
	// TODO: maybe an error
}

func (mt *MapTask) WorkerFailed() {
	if mt.state == Assigned {
		mt.state = Unassigned
	}
	// TODO: maybe an error
}

type TaskState int

const (
	Unassigned TaskState = iota
	Assigned
	Done
)

func NewMapTask(path string) MapTask {
	return MapTask{
		path:  MapTaskFilePath(path),
		state: Unassigned,
	}
}

type MapTaskFilePath string

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
