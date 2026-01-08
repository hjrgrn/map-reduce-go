package types

import (
	"fmt"
	"log"
	"mapreduce/pkg/mr"
	"net/netip"
	"net/rpc"
)

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
