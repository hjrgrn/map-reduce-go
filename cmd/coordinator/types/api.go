package types

//
// `Coordinator`'s methods that will be served through RPCs.
//

import (
	"mapreduce/pkg/mr"
)

// an example RPC handler.
func (c *Coordinator) Example(args *mr.ExampleArgs, reply *mr.ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// XXX: A RPC handler that assisgns a Task to a Worker requiring it.
func (c *Coordinator) GetTask(args *mr.GetTaskArgs, reply *mr.GetTaskReply) error {
	if c.state == Map {
		c.mutex.Lock()

		n_task := len(c.map_tasks)
		for i := range n_task {
			c.cursor = (c.cursor + i) % n_task
			element := c.map_tasks[c.cursor]
			if element.state == Pending {
				map_reply := mr.GetMapTaskReply{
					Path:           element.path,
					Buckets:        c.buckets,
					Index:          c.cursor,
					MapIsCompleted: false,
				}
				reply.MapReply = &map_reply
				return nil
			}
		}
		// No pending tasks
		c.state = Reduce

		c.mutex.Unlock()
	} else if c.state == Reduce {
		// TODO:
	} else {
		// XXX:
	}

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
