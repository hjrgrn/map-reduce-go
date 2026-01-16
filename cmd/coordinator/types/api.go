package types

//
// `Coordinator`'s methods that will be served through RPCs.
//

import "mapreduce/pkg/mr"

// an example RPC handler.
func (c *Coordinator) Example(args *mr.ExampleArgs, reply *mr.ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// XXX: A RPC handler that assisgns a Task to a Worker requiring it.
func (c *Coordinator) GetTask(args *mr.GetTaskArgs, reply *mr.GetTaskReply) error {
outer:
	for {
		if c.state == Map {
			// TODO: add a timer
			c.mutex.Lock()
			for k, v := range c.map_tasks {
				if v.state == TaskPending {
					map_reply := mr.GetMapTaskReply{
						Path:    k,
						Buckets: c.buckets,
					}
					c.map_tasks[k] = v
					reply.MapReply = &map_reply
					continue outer
				}
			}
			c.mutex.Unlock()
			c.state = Reduce
		} else if c.state == Reduce {
			// XXX:
			c.state = Completed
		} else {
			// XXX:
			break
		}
	}

	return nil
}

// A RPC handler that a Map Worker calls to communicate the Coordinator that has
// completed its assigned task.
func (c *Coordinator) MapCompleted(args *mr.MapCompletedArgs, reply *mr.MapCompletedReply) error {
	c.mutex.Lock()
	c.map_tasks[args.Path].Done(args.Addr)
	defer c.mutex.Unlock()
	return nil
}
