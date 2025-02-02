package main

import (
	"context"
	operations "nrMQ/kitex_gen/api"
)

// ZkServer_OperationImpl implements the last service interface defined in the IDL.
type ZkServer_OperationImpl struct{}

// CreateTopic implements the ZkServer_OperationImpl interface.
func (s *ZkServer_OperationImpl) CreateTopic(ctx context.Context, req *operations.CreateTopicRequest) (resp *operations.CreateTopicResponse, err error) {
	// TODO: Your code here...
	return
}
