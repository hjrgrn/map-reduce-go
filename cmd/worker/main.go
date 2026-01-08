package main

//
// Start a worker process.
//
// go run ./cmd/worker/main.go  XXX:
//

import (
	"fmt"
	"mapreduce/cmd/worker/types"
	"os"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrworker <APP>\n")
		os.Exit(1)
	}

	mapf, reducef := types.LoadApp(os.Args[1])

	worker := types.Worker{}

	worker.Work(mapf, reducef)
}
