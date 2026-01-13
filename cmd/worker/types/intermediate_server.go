package types

import "mapreduce/pkg/mr"

//
// This subpackage contains logic for the RPC server that serves intermediate files
// from map workers to reduce workers.
//

type Server struct {
	w *Worker
}

// An RPC handler that sends an intermediate file to a requesting reduce worker.
// Reply.success will be false if there are no intermediate files with the given
// id provided through GetBucketArgs.id, this should not happen.
func (s *Server) GetBucket(args *GetBucketArgs, reply *GetBucketReply) error {
	path, success := s.w.getBucketFromId(args.Id)
	reply.Path = path
	reply.success = success
	return nil
}

//
// GetBucket RPC arguments and reply.
//

type GetBucketArgs struct {
	Id int
}

type GetBucketReply struct {
	// TODO: we are working on the same filesystem, in the future we will have to
	// pass the entire file.
	Path    mr.IntermediateFilePath
	// Communicates if there is an intermediate file identified by GetBucketArgs.Id.
	// This should always be true.
	success bool
}
