package types

import (
	"io"
	"log"
	"mapreduce/pkg/utils"
	"net"
	"os"
	"strconv"
	"sync"
)

//
// XXX:
//

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
