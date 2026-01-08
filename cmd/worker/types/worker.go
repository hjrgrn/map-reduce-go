package types

import (
	"io"
	"log"
	"mapreduce/pkg/mr"
	"mapreduce/pkg/utils"
	"net"
	"os"
	"strconv"
	"sync"
)

//
// worker subpackage.
//

// Manages the state of the worker.
type Worker struct {
	// Protects access to `state`.
	mutex sync.Mutex
	// State of `Worker`.
	state WorkerState
	// Intermediate files.
	buckets map[int]Bucket
}

// Instantiates a `Worker`.
func NewWorker() Worker {
	return Worker{buckets: make(map[int]Bucket)}
}

// Type that represents the state of Worker.
// The possible values are `WorkingOnMap` (default), `WorkingOnReduce` or `Done`.
type WorkerState int

const (
	WorkingOnMap WorkerState = iota
	WorkingOnReduce
	// When `Worker` is in this state, there is no more work to do; the app should
	// shut down.
	Done
)

// Asks the server for a Map/Reduce Task and executes `mapf` and `reducef` in a
// separate routine using the value received, according to the state of `Worker`.
// When there is no more work to do (`Worker.state` is `Done`), the function returns.
// TODO: mapf and reducef should be fields of Worker
func (w *Worker) Work(mapf func(string, string) utils.ByKey,
	reducef func(string, []string) string) {
	for {
		w.mutex.Lock()
		state := w.state
		w.mutex.Unlock()

		if state == Done {
			break
		} else if state == WorkingOnMap {
			go w.runMap(mapf)
		} else {
			// TODO:
			w.mutex.Lock()
			w.state = Done
			w.mutex.Unlock()
		}
		// TODO: close buckets files
	}

}

// Runs the map task. It's supposed to be run in a separate routine.
func (w *Worker) runMap(mapf func(string, string) utils.ByKey,
) {
	reply, ok := CallGetMapTask()
	for !ok {
		// TODO: wait a certain amount of time before retrying, retry only a certain
		// amount of time
		reply, ok = CallGetMapTask()
	}

	if reply.MapIsCompleted {
		w.mutex.Lock()
		w.state = WorkingOnReduce
		w.mutex.Unlock()
	} else {
		// TODO: we are working on the same filesystem at the moment
		filename := string(reply.Path)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))

		w.saveIntermediateFiles(kva, &reply)
		w.launchRPCServer(&reply)
	}
}

// Save intermediate files into `Worker.buckets`
func (w *Worker) saveIntermediateFiles(kva utils.ByKey, reply *mr.GetMapTaskReply) {
	for i := range reply.Buckets {
		w.buckets[i] = NewBucket(i)
	}
	for _, kv := range kva {
		// TODO: error handling
		index, _ := strconv.Atoi(kv.Key)
		hash := index % reply.Buckets
		writer := w.buckets[hash].Writer
		// TODO: error handling
		writer.Write([]string{kv.Key, kv.Value})
		writer.Flush()
	}
}

// Launch an RPC server that will serve intermediate files to Reduce Workers.
func (w *Worker) launchRPCServer(reply *mr.GetMapTaskReply) {
	// TODO: Setup RPCs for other clients

	l, e := net.Listen("tcp", ":0")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	addr := l.Addr().(*net.TCPAddr)
	addr_port := addr.AddrPort()
	CallMapCompleted(reply.Path, addr_port)

	// TODO: delete this
	l.Close()
}
