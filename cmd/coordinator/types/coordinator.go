package types

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
)

// Manages the main coordination logic for map and reduce tasks.
type Coordinator struct {
	// Protects access to the `map_tasks` field.
	mutex sync.Mutex

	// Holds the tasks to be assigned to Map Workers,
	// keyed by input file path. Each Map Worker processes one file.
	map_tasks []*MapTask

	// XXX: The number of intermediate file buckets produced by Map Workers.
	// Each Reduce Worker is assigned one bucket and collects the corresponding
	// intermediate files from all Map Workers.
	buckets []*Bucket

	// XXX:
	state CoordinatorState

	///XXX:
	map_cursor int

	//XXX:
	reduce_cursor int
}

// `MakeCoordinator` helper function.
// Initializes a Coordinator instance.
func build_coordinator(files []string, nBuckets int) Coordinator {
	buckets := make([]*Bucket, nBuckets)
	tasks := make([]*MapTask, len(files))
	for i := range files {
		task := NewMapTask(files[i])
		tasks[i] = &task
	}

	return Coordinator{map_tasks: tasks, buckets: buckets}
}

// main calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// XXX:

	return ret
}

// Create a Coordinator.
// `main` calls this function.
// `buckets` is the number of reduce tasks to use.
func MakeCoordinator(files []string, buckets int) *Coordinator {
	c := build_coordinator(files, buckets)

	// XXX:

	c.server()
	return &c
}

// Registers Coordinator and starts a routine that listens for RPCs from worker.go.
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":5000")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	// TODO: add channel for graceful shutdown
	go http.Serve(l, nil)
}
