package main

//
// Start a worker process.
//
// go run ./cmd/worker/main.go  XXX:
//

import (
	"fmt"
	"log"
	"mapreduce/pkg/mr"
	"mapreduce/pkg/utils"
	"net/rpc"
	"os"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrworker <APP>\n")
		os.Exit(1)
	}

	mapf, reducef := loadApp(os.Args[1])

	Worker(mapf, reducef)
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

// Send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args any, reply any) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":5000")
	if err != nil {
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

// main calls this function.
func Worker(mapf func(string, string) utils.ByKey,
	reducef func(string, []string) string) {

	// XXX:

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
