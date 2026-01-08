package types

import (
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"
)

type Bucket struct {
	Id     int
	Path   string
	File   *os.File
	Writer *csv.Writer
	Reader *csv.Reader
}

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
	return Bucket{Id: id, Path: path, File: file, Writer: writer, Reader: reader}
}
