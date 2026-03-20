package types

import (
	"fmt"
	"io"
	"log"
	"mapreduce/pkg/mr"
	"mapreduce/pkg/utils"
	"net"
	"net/http"
	"net/rpc"
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
	state utils.State
	// Intermediate files.
	buckets map[int]Bucket
}

// Instantiates a `Worker`.
func NewWorker() Worker {
	return Worker{buckets: make(map[int]Bucket)}
}

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

		if state == utils.Completed {
			// TODO: graceful shutdown
			fmt.Println("The work is completed.")
			os.Exit(0)
		} else if state == utils.Map {
			go w.runMap(mapf)
		} else if state == utils.Reduce {
			go w.runReduce(reducef)
		} else {
			// TODO:
			w.mutex.Lock()
			w.state = utils.Completed
			w.mutex.Unlock()
		}
		// TODO: close buckets files
	}

}

// Runs the map task. It's supposed to be run in a separate routine.
// If the map task is completed the internal state of Worker will be changed to Reduce.
// This function tries to contact the coordinator 3 times, if after these tries the coordinator
// hasn't responded yet this function will crash the process.
func (w *Worker) runMap(mapf func(string, string) utils.ByKey,
) {
	reply, ok := CallGetMapTask()
	for range 3 {
		if ok {
			break
		}
		// TODO: wait a certain amount of time before retrying
		reply, ok = CallGetMapTask()
	}
	if !ok {
		log.Fatalln("The coordinator seems to be offline.")
	}

	if reply.MapIsCompleted {
		w.mutex.Lock()
		w.state = utils.Reduce
		w.mutex.Unlock()
	} else {
		// TODO: we are working on the same filesystem at the moment
		filename := string(reply.Path)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		defer file.Close()
		content, err := io.ReadAll(file)
		if err != nil {
			log.Panicf("cannot read %v", filename)
		}
		kva := mapf(filename, string(content))

		w.saveIntermediateFiles(kva, &reply)
		go w.launchRPCServer(&reply)
	}
}

func (w *Worker) runReduce(reducef func(string, []string) string) {
	reply, ok := CallGetReduceTask()
	for !ok {
		// TODO: wait a certain amount of time before retrying, retry only a certain
		// amount of time
		reply, ok = CallGetReduceTask()
	}

	if reply.ReduceIsCompleted {
		w.mutex.Lock()
		w.state = utils.Completed
		w.mutex.Unlock()
	} else {
		buckets := make([]string, 0, len(reply.Addresses))
		bucket_id := reply.Bucket
		for _, addr := range reply.Addresses {
			// TODO: duplication in call()
			c, err := rpc.DialHTTP("tcp", addr.String())
			if err != nil {
				// TODO: graceful handling of DialHTTP failure, graceful shutdown
				log.Fatal("dialing:", err)
			}
			defer c.Close()
			args := GetBucketArgs{
				Id: bucket_id,
			}
			reply := GetBucketReply{}
			err = c.Call("IntermediateServer.GetBucket", args, reply)
			if reply.Success {
				// TODO:
			}
			if err != nil {
				// TODO: maybe retry
				log.Fatal("TODO:", err)
			}
			file, err := os.Open(string(reply.Path))
			if err != nil {
				// TODO: maybe retry
				log.Fatal("TODO:", err)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				// TODO:
			}
			buckets = append(buckets, string(content))
		}
		// TODO: fix this
		reducef(string(bucket_id), buckets)
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
	s := IntermediateServer{w}
	rpc.Register(s)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":0")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	addr := l.Addr().(*net.TCPAddr)
	addr_port := addr.AddrPort()
	CallMapCompleted(reply.Index, addr_port)

	http.Serve(l, nil)
}

// Returns the path of a packet identified by id, and a bool indicating success.
// The bool is false if no bucket is found with the given id.
func (w *Worker) getBucketFromId(id int) (mr.IntermediateFilePath, bool) {
	bucket, ok := w.buckets[id]
	return bucket.Path, ok
}
