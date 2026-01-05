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
	for k, v := range c.map_tasks {
		if v.state == Unassigned {
			reply.Path = k
			reply.Buckets = c.buckets
			v.Assign()
			c.map_tasks[k] = v
			reply.MapIsCompleted = false
			return nil
		}
	}
	reply.MapIsCompleted = true

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
