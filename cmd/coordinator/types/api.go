package types

//
// `Coordinator`'s methods that will be served through RPCs.
//

import (
	"mapreduce/pkg/mr"
	"math/rand"
	"net/netip"
)

// an example RPC handler.
func (c *Coordinator) Example(args *mr.ExampleArgs, reply *mr.ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// XXX: A RPC handler that assisgns a Task to a Worker requiring it.
func (c *Coordinator) GetMapTask(args *mr.GetMapTaskArgs, reply *mr.GetMapTaskReply) error {
	if c.state != Map {
		reply.MapIsCompleted = true
		return nil
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()

	n_task := len(c.map_tasks)
	for i := range n_task {
		c.map_cursor = (c.map_cursor + i) % n_task
		element := c.map_tasks[c.map_cursor]
		if element.state == Pending {
			reply.Path = element.path
			reply.Buckets = len(c.buckets)
			reply.Index = c.map_cursor
			reply.MapIsCompleted = false
			return nil
		}
	}
	// No pending tasks
	c.state = Reduce
	reply.MapIsCompleted = true
	return nil
}

func (c *Coordinator) GetReduceTask(args *mr.GetReduceTaskArgs, reply *mr.GetReduceTaskReply) error {
	if c.state != Reduce {
		// TODO:
		return nil
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	n_buckets := len(c.buckets)
	for i := range n_buckets {
		c.reduce_cursor = (c.reduce_cursor + i) % n_buckets
		if c.buckets[i].state == Pending {
			addresses := make([]*netip.AddrPort, len(c.map_tasks))
			for i, element := range c.map_tasks {
				addresses[i] = element.addr
			}
			reply.Addresses = addresses
			reply.Bucket = i
			reply.ReduceIsCompleted = false
			return nil
		}
	}
	reply.ReduceIsCompleted = false

	return nil
}

// A RPC handler that a Map Worker calls to communicate the Coordinator that has
// completed its assigned task.
func (c *Coordinator) MapCompleted(args *mr.MapCompletedArgs, reply *mr.MapCompletedReply) error {
	c.mutex.Lock()
	c.map_tasks[args.Index].Done(args.Addr)
	defer c.mutex.Unlock()
	return nil
}
