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

// A RPC handler that assisgns a Map Task to a Map Worker requiring it.
func (c *Coordinator) GetMapTask(args *mr.GetMapTaskArgs, reply *mr.GetMapTaskReply) error {
	// TODO: add a timer
	c.mutex.Lock()
	defer c.mutex.Unlock()
	assigned := false
	for k, v := range c.map_tasks {
		if v.state == Unassigned {
			reply.Path = k
			reply.Buckets = c.buckets
			assigned = true
			v.Assign()
			c.map_tasks[k] = v
			reply.MapIsCompleted = false
			break
		}
	}
	if !assigned {
		reply.MapIsCompleted = true
	}

	return nil
}

// XXX:
func (c *Coordinator) MapCompleted(args *mr.MapCompletedArgs, reply *mr.MapCompletedReply) error {
	c.mutex.Lock()
	c.map_tasks[args.Path].Done(args.Addr)
	defer c.mutex.Unlock()
	return nil
}
