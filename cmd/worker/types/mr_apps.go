package types

import (
	"fmt"
	"log"
	"mapreduce/pkg/utils"
)

// Load the application Map and Reduce functions
func LoadApp(appname string) (func(string, string) utils.ByKey, func(string, []string) string) {
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
