package utils

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// For sorting by key.
type ByKey []KeyValue

// For sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
