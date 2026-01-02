package main

//
// Start a worker process.
//
// go run ./cmd/worker/main.go  XXX:
//

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"mapreduce/pkg/mr"
	"mapreduce/pkg/utils"
	"net/netip"
	"net/rpc"
	"os"
	"strconv"
	"sync"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrworker <APP>\n")
		os.Exit(1)
	}

	mapf, reducef := loadApp(os.Args[1])

	worker := Worker{}

	worker.Work(mapf, reducef)
}

// XXX:
type Worker struct {
	mutex sync.Mutex
	state WorkerState
}

// XXX:
type WorkerState int

const (
	WorkingOnMap WorkerState = iota
	WorkingOnReduce
	Done
)

// XXX:
func (w *Worker) Work(mapf func(string, string) utils.ByKey,
	reducef func(string, []string) string) {
	for {
		w.mutex.Lock()
		state := w.state
		w.mutex.Unlock()

		if state == Done {
			break
		} else if state == WorkingOnMap {
			go func() {
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

					// save files
					buckets := make(map[int]Bucket, reply.Buckets)
					for i := range reply.Buckets {
						buckets[i] = NewBucket(i)
					}
					for _, kv := range kva {
						// TODO: error handling
						index, _ := strconv.Atoi(kv.Key)
						hash := index % reply.Buckets
						writer := buckets[hash].Writer
						// TODO: error handling
						writer.Write([]string{kv.Key, kv.Value})
						writer.Flush()
					}

					// TODO: open a server

					// TODO: placeholder
					addr := netip.MustParseAddr("127.0.0.1")
					port := uint16(0)
					placeholder_addr := netip.AddrPortFrom(addr, port)

					CallMapCompleted(reply.Path, placeholder_addr)
				}
			}()
		} else {
			// TODO:
			w.mutex.Lock()
			w.state = Done
			w.mutex.Unlock()
		}
		// TODO: close buckets files
	}

}

type Bucket struct {
	Id     int
	Path   string
	File   *os.File
	Writer *csv.Writer
	Reader *csv.Reader
}

func NewBucket(id int) Bucket {
	// TODO: Path, maybe use "/var/tmp"
	path := "./instace/" + strconv.Itoa(os.Getegid()) + "-" + strconv.Itoa(id)
	file, err := os.Create(path)
	writer := csv.NewWriter(file)
	reader := csv.NewReader(file)
	// TODO: maybe use encoding/gob
	if err != nil {
		fmt.Printf("Fatal error opening a file: %v", err)
		os.Exit(2)
	}
	return Bucket{Id: id, Path: path, File: file, Writer: writer, Reader: reader}
}

// Load the application Map and Reduce functions
func loadApp(appname string) (func(string, string) utils.ByKey, func(string, []string) string) {
	var mapf func(string, string) utils.ByKey
	var reducef func(string, []string) string
	if appname == "wordcount" || appname == "wc" {
		fmt.Println("Executing wordcount..")
		mapf = Map
		reducef = Reduce
	} else {
		log.Fatalf("Unsupported application.")
	}

	return mapf, reducef
}

// XXX: Call example
func CallExample() {
	// declare an argument structure.
	args := mr.ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := mr.ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// XXX:
func CallGetMapTask() (mr.GetMapTaskReply, bool) {
	// declare an argument structure.
	args := mr.GetMapTaskArgs{}

	// declare a reply structure.
	reply := mr.GetMapTaskReply{}

	// send the RPC request, wait for the reply.
	ok := call("Coordinator.GetMapTask", &args, &reply)

	return reply, ok
}

// XXX:
func CallMapCompleted(filename mr.MapTaskFilePath, addr netip.AddrPort) (mr.MapCompletedReply, bool) {
	args := mr.MapCompletedArgs{
		Path: filename,
		Addr: addr,
	}
	reply := mr.MapCompletedReply{}
	ok := call("Coordinator.MapCompleted", &args, &reply)

	return reply, ok
}

// Send an RPC request to the coordinator and wait for the response.
// If it fails to contact the coordinator, the program will crash.
// If something else goes wrong, it returns false.
// If successful, it returns true.
func call(rpcname string, args any, reply any) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":5000")
	if err != nil {
		// TODO: graceful handling of DialHTTP failure, graceful shutdown
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// XXX: Make MR applications plugins
func Map(filename string, contents string) utils.ByKey {
	// XXX:
	return utils.ByKey{}
}

// XXX: Make MR applications plugins
func Reduce(key string, values []string) string {
	// XXX:
	return "TODO"
}
