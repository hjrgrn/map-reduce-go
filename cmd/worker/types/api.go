package types

//
// API subpackage.
// This subpackage contains calls that `Workers` performs against the
// coordinator server.
//

import (
	"fmt"
	"log"
	"mapreduce/pkg/mr"
	"mapreduce/pkg/utils"
	"net/netip"
	"net/rpc"
	"time"
)

// Checks if the server is online.
// Example RPC client call.
func CallCheckHealth() {
	// declare an argument structure.
	args := mr.CheckHealthArgs{}

	// declare a reply structure.
	reply := mr.CheckHealthReply{}

	// send the RPC request, wait for the reply.
	// The "Coordinator.CheckHealth" tells the
	// receiving server that we'd like to call
	// the CheckHealth() method of struct Coordinator.
	ok := call("Coordinator.CheckHealth", &args, &reply)

	if ok {
		var state string
		if reply.State == utils.Map {
			state = "Map"
		} else if reply.State == utils.Reduce {
			state = "Reduce"
		} else {
			state = "Done"
		}

		fmt.Println("Coordinator is online.\nUptime: ", reply.Uptime.Truncate(time.Second), "\nState: ", state)
	} else {
		fmt.Println("Coordinator is unreachable.")
	}
}

// Calls `Coordinator.GetMapTask` on the RPC server offered by the coordinator.
// If everything goes fine, the server will respond with a Map Task to work on.
// Returns the reply and a bool, if something went wrong the bool will be false,
// it will be true otherwise.
func CallGetMapTask() (mr.GetMapTaskReply, bool) {
	// declare an argument structure.
	args := mr.GetMapTaskReply{}

	// declare a reply structure.
	reply := mr.GetMapTaskReply{}

	// send the RPC request, wait for the reply.
	ok := call("Coordinator.GetMapTask", &args, &reply)

	return reply, ok
}

// TODO: code duplication CallGetMapTask
func CallGetReduceTask() (mr.GetReduceTaskReply, bool) {
	// declare an argument structure.
	args := mr.GetReduceTaskArgs{}

	// declare a reply structure.
	reply := mr.GetReduceTaskReply{}

	// send the RPC request, wait for the reply.
	ok := call("Coordinator.GetReduceTask", &args, &reply)

	return reply, ok
}

// Calls `Coordinator.MapCompleted` on the RPC server offered by the coordinator.
// This call communicates that the Map Task assigned has been completed, and the
// worker is ready to serve intermediate files, on the address and port provided
// by `addr`, to Reduce Workers.
// Returns the reply and a bool, The bool is true if everything went fine, false
// otherwise.
func CallMapCompleted(index int, addr netip.AddrPort) (mr.MapCompletedReply, bool) {
	args := mr.MapCompletedArgs{
		Index: index,
		Addr:  addr,
	}
	reply := mr.MapCompletedReply{}
	ok := call("Coordinator.MapCompleted", &args, &reply)

	return reply, ok
}

// XXX:
func CallReduceCompleted(bucket int) (mr.ReduceCompletedReply, bool) {
	args := mr.ReduceCompletedArgs{
		Bucket: bucket,
	}
	reply := mr.ReduceCompletedReply{}
	ok := call("Coordinator.ReduceCompleted", &args, &reply)
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
	// TODO: Why aren't we returning the error?
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
