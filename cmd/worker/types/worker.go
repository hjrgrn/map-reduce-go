package types

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"mapreduce/pkg/mr"
	"mapreduce/pkg/utils"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
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
	// This guard ensures the map functionality is not executed multiple times.
	// TODO: maybe that is not the best plan
	init := true
	for {
		w.mutex.Lock()
		state := w.state
		w.mutex.Unlock()

		// TODO: it's a busy loop, maybe use sync.Cond
		if state == utils.Map {
			// NOTE: the map function will be executed only once, at the end of the map procedure
			// only one set of intermediate files will be present on the filesystem.
			go w.runMap(mapf, &init)
		} else if state == utils.Reduce {
			// NOTE: the reduce function may be executed multiple times
			go w.runReduce(reducef)
		} else {
			// TODO: graceful shutdown
			fmt.Println("The work is completed.")
			os.Exit(0)
		}
		// XXX:
		time.Sleep(1 * time.Second)
		// TODO: close buckets files
	}

}

// Runs the map task. It's supposed to be run in a separate routine.
// If the map task is completed the internal state of Worker will be changed to Reduce.
// This function tries to contact the coordinator 3 times, if after these tries the coordinator
// hasn't responded yet this function will crash the process.
// TODO: describe init and change init name
func (w *Worker) runMap(mapf func(string, string) utils.ByKey,
	init *bool) {
	// TODO: check error message when coordinator is online
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

	w.mutex.Lock()
	w.state = reply.State
	state := w.state
	w.mutex.Unlock()

	if state == utils.Map && *init == true {
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
		*init = false
	}
}

func (w *Worker) runReduce(reducef func(string, []string) string) {
	var reply mr.GetReduceTaskReply
	var ok bool
	for range 3 {
		// TODO: check error message when coordinator is online
		reply, ok = CallGetReduceTask()
		if ok {
			break
		}
		// TODO: wait a certain amount of time before retrying
		time.Sleep(1 * time.Second)
	}
	if !ok {
		log.Fatalln("The coordinator seems to be offline.")
	}

	w.mutex.Lock()
	w.state = reply.State
	state := w.state
	w.mutex.Unlock()

	if state == utils.Reduce {
		buckets := make([][][]string, 0, len(reply.Addresses))
		var bucket_id int
		if reply.Bucket != nil {
			bucket_id = *reply.Bucket
		} else {
			// TODO: explain: RPC struggles with int values, we are sure that bucket_id is
			// 0 since we are in Reduce state
			bucket_id = 0
		}
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
			err = c.Call("IntermediateServer.GetBucket", args, &reply)
			if err != nil {
				// TODO: maybe retry
				log.Fatal("TODO:", err)
			}
			if !reply.Success {
				// TODO: We have asked for a bucket_id that is not registered
				log.Fatal("TODO:", err)
			}
			file, err := os.Open(string(reply.Path))
			reader := csv.NewReader(file)
			records, err := reader.ReadAll()
			if err != nil {
				// TODO: maybe retry
				log.Fatal("TODO:", err)
			}
			buckets = append(buckets, records)
		}

		// Each bucket maintains internal alphabetical order, however, the buckets
		// themselves are not globally ordered.
		// A key that is present in a bucket cannot be present in other buckets.
		final := make(utils.ByKey, 0)
		var current_key string
		values := make([]string, 0)
		// TODO: better name
		guard := true

		for _, bucket := range buckets {
			for _, row := range bucket {
				if guard {
					current_key = row[0]
					guard = false
				}
				if row[0] == current_key {
					values = append(values, row[1])
				} else {
					value := reducef(current_key, values)
					final = append(final, utils.KeyValue{Key: current_key, Value: value})
					current_key = row[0]
					values = make([]string, 0)
					values = append(values, row[1])
				}
			}
		}
		saveFinalFile(final, bucket_id)

		// TODO: error handling
		CallReduceCompleted(bucket_id)
	}
}

// XXX:
func saveFinalFile(final utils.ByKey, bucket int) {
	sort.Sort(final)
	path := "./instance/bucket-" + strconv.Itoa(bucket)
	file, err := os.Create(path)
	// TODO: error handling
	if err != nil {
		fmt.Printf("Fatal error opening a file: %v", err)
		os.Exit(2)
	}
	writer := csv.NewWriter(file)
	for _, kv := range final {
		writer.Write([]string{kv.Key, kv.Value})
	}

}

// Save intermediate files into `Worker.buckets`
// Results are shuffled and sorted into the buckets thanks to a very simple hash function.
func (w *Worker) saveIntermediateFiles(kva utils.ByKey, reply *mr.GetMapTaskReply) {
	sort.Sort(kva)
	for i := range reply.Buckets {
		w.buckets[i] = NewBucket(i)
	}
	for _, kv := range kva {
		// TODO: error handling
		index := HashFunction(kv.Key, reply.Buckets)
		writer := w.buckets[index].Writer
		// TODO: error handling
		writer.Write([]string{kv.Key, kv.Value})
		writer.Flush()
	}
}

// Launch an RPC server that will serve intermediate files to Reduce Workers.
func (w *Worker) launchRPCServer(reply *mr.GetMapTaskReply) {
	// TODO: Setup RPCs for other clients
	s := IntermediateServer{w}
	rpc.Register(&s)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	addr := l.Addr().(*net.TCPAddr)
	addr_port := addr.AddrPort()
	// TODO: error handling
	CallMapCompleted(reply.Index, addr_port)

	http.Serve(l, nil)
}

// Returns the path of a packet identified by id, and a bool indicating success.
// The bool is false if no bucket is found with the given id.
func (w *Worker) getBucketFromId(id int) (mr.IntermediateFilePath, bool) {
	bucket, ok := w.buckets[id]
	return bucket.Path, ok
}
