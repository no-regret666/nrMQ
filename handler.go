package main

import (
	"context"
	api "nrMQ/kitex_gen/api"
	operations "nrMQ/kitex_gen/api"
)

// ZkServer_OperationImpl implements the last service interface defined in the IDL.
type ZkServer_OperationImpl struct{}

// CreateTopic implements the ZkServer_OperationImpl interface.
func (s *ZkServer_OperationImpl) CreateTopic(ctx context.Context, req *operations.CreateTopicRequest) (resp *operations.CreateTopicResponse, err error) {
	// TODO: Your code here...
	return
}

// CreatePart implements the ZkServer_OperationImpl interface.
func (s *ZkServer_OperationImpl) CreatePart(ctx context.Context, req *operations.CreatePartRequest) (resp *operations.CreatePartResponse, err error) {
	// TODO: Your code here...
	return
}

// ProGetBroker implements the ZkServer_OperationImpl interface.
func (s *ZkServer_OperationImpl) ProGetBroker(ctx context.Context, req *api.ProGetBrokRequest) (resp *api.ProGetBrokResponse, err error) {
	// TODO: Your code here...
	return
}

// SetPartitionState implements the ZkServer_OperationImpl interface.
func (s *ZkServer_OperationImpl) SetPartitionState(ctx context.Context, req *api.SetPartitionStateRequest) (resp *api.SetPartitionStateResponse, err error) {
	// TODO: Your code here...
	return
}

// Pub implements the Client_OperationsImpl interface.
func (s *Client_OperationsImpl) Pub(ctx context.Context, req *api.PubRequest) (resp *api.PubResponse, err error) {
	// TODO: Your code here...
	return
}

// Pingpong implements the Client_OperationsImpl interface.
func (s *Client_OperationsImpl) Pingpong(ctx context.Context, req *api.PingPongRequest) (resp *api.PingPongResponse, err error) {
	// TODO: Your code here...
	return
}
