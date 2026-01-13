package types

//
// bucket subpackage.
// This subpackage provides the struct `Bucket` and relative functionalities.
//

import (
	"encoding/csv"
	"fmt"
	"mapreduce/pkg/mr"
	"math/rand"
	"os"
	"strconv"
	"time"
)

// Handles an intermediate file.
type Bucket struct {
	// Identifies the intermediate file.
	Id int
	// Path to the intermediate file.
	Path mr.IntermediateFilePath
	// File abstraction connected to the file.
	File *os.File
	// Writer pointing to the file.
	Writer *csv.Writer
	// Reader pointing to the file.
	Reader *csv.Reader
}

// Instantiate a new Bucket.
// TODO: for the time being the intermediate file is saved in the directory
// `instance`, using a somewhat random filepath.
func NewBucket(id int) Bucket {
	// TODO: Path, maybe use "/var/tmp"
	rand.New(rand.NewSource(time.Now().UnixNano()))
	// TODO: use path/filepath
	path := "./instace/" + strconv.Itoa(os.Getegid()) + "-" + strconv.Itoa(id) + "-" + strconv.Itoa(rand.Intn(1000000))
	file, err := os.Create(path)
	writer := csv.NewWriter(file)
	reader := csv.NewReader(file)
	// TODO: maybe use encoding/gob
	if err != nil {
		fmt.Printf("Fatal error opening a file: %v", err)
		os.Exit(2)
	}
	return Bucket{Id: id, Path: mr.IntermediateFilePath(path), File: file, Writer: writer, Reader: reader}
}
