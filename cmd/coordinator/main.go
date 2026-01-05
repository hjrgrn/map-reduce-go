package main

//
// Start the coordinator process
//
// Usage:
//
// ```bash
// go run ./cmd/coordinator/main.go \
//  ./testdata/pg-sherlock_holmes.txt \
// 	./testdata/pg-grim.txt \
//  ./testdata/frankenstein.txt
// ```

import (
	"fmt"
	"mapreduce/cmd/coordinator/types"
	"os"
	"strconv"
	"time"
)

func main() {
	usage := "Usage: coordinator buckets inputfiles...\n"
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, usage)
		os.Exit(1)
	}

	buckets, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, usage)
		os.Exit(1)
	}

	m := types.MakeCoordinator(os.Args[2:], buckets)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
