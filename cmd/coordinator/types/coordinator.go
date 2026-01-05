package types

import (
	"log"
	"mapreduce/pkg/mr"
	"net"
	"net/http"
	"net/rpc"
	"sync"
)

type Coordinator struct {
	mutex     sync.Mutex
	map_tasks map[mr.MapTaskFilePath]*MapTask
	buckets   int
}

// XXX:
func build_coordinator(files []string, buckets int) Coordinator {
	tasks := make(map[mr.MapTaskFilePath]*MapTask, len(files))
	for i := range files {
		task := NewMapTask(files[i])
		tasks[task.path] = &task
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
