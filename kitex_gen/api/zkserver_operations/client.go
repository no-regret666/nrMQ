// Code generated by Kitex v0.12.1. DO NOT EDIT.

package zkserver_operations

import (
	"context"
	client "github.com/cloudwego/kitex/client"
	callopt "github.com/cloudwego/kitex/client/callopt"
	api "nrMQ/kitex_gen/api"
)

// Client is designed to provide IDL-compatible methods with call-option parameter for kitex framework.
type Client interface {
	CreateTopic(ctx context.Context, req *api.CreateTopicRequest, callOptions ...callopt.Option) (r *api.CreateTopicResponse, err error)
	CreatePart(ctx context.Context, req *api.CreatePartRequest, callOptions ...callopt.Option) (r *api.CreatePartResponse, err error)
	SetPartitionState(ctx context.Context, req *api.SetPartitionStateRequest, callOptions ...callopt.Option) (r *api.SetPartitionStateResponse, err error)
	ProGetLeader(ctx context.Context, req *api.ProGetLeaderRequest, callOptions ...callopt.Option) (r *api.ProGetLeaderResponse, err error)
	Sub(ctx context.Context, req *api.SubRequest, callOptions ...callopt.Option) (r *api.SubResponse, err error)
	ConStartGetBroker(ctx context.Context, req *api.ConStartGetBrokRequest, callOptions ...callopt.Option) (r *api.ConStartGetBrokResponse, err error)
	BroInfo(ctx context.Context, req *api.BroInfoRequest, callOptions ...callopt.Option) (r *api.BroInfoResponse, err error)
	UpdateRep(ctx context.Context, req *api.UpdateRepRequest, callOptions ...callopt.Option) (r *api.UpdateRepResponse, err error)
	UpdatePTPOffset(ctx context.Context, req *api.UpdatePTPOffsetRequest, callOptions ...callopt.Option) (r *api.UpdatePTPOffsetResponse, err error)
}

// NewClient creates a client for the service defined in IDL.
func NewClient(destService string, opts ...client.Option) (Client, error) {
	var options []client.Option
	options = append(options, client.WithDestService(destService))

	options = append(options, opts...)

	kc, err := client.NewClient(serviceInfoForClient(), options...)
	if err != nil {
		return nil, err
	}
	return &kZkServer_OperationsClient{
		kClient: newServiceClient(kc),
	}, nil
}

// MustNewClient creates a client for the service defined in IDL. It panics if any error occurs.
func MustNewClient(destService string, opts ...client.Option) Client {
	kc, err := NewClient(destService, opts...)
	if err != nil {
		panic(err)
	}
	return kc
}

type kZkServer_OperationsClient struct {
	*kClient
}

func (p *kZkServer_OperationsClient) CreateTopic(ctx context.Context, req *api.CreateTopicRequest, callOptions ...callopt.Option) (r *api.CreateTopicResponse, err error) {
	ctx = client.NewCtxWithCallOptions(ctx, callOptions)
	return p.kClient.CreateTopic(ctx, req)
}

func (p *kZkServer_OperationsClient) CreatePart(ctx context.Context, req *api.CreatePartRequest, callOptions ...callopt.Option) (r *api.CreatePartResponse, err error) {
	ctx = client.NewCtxWithCallOptions(ctx, callOptions)
	return p.kClient.CreatePart(ctx, req)
}

func (p *kZkServer_OperationsClient) SetPartitionState(ctx context.Context, req *api.SetPartitionStateRequest, callOptions ...callopt.Option) (r *api.SetPartitionStateResponse, err error) {
	ctx = client.NewCtxWithCallOptions(ctx, callOptions)
	return p.kClient.SetPartitionState(ctx, req)
}

func (p *kZkServer_OperationsClient) ProGetLeader(ctx context.Context, req *api.ProGetLeaderRequest, callOptions ...callopt.Option) (r *api.ProGetLeaderResponse, err error) {
	ctx = client.NewCtxWithCallOptions(ctx, callOptions)
	return p.kClient.ProGetLeader(ctx, req)
}

func (p *kZkServer_OperationsClient) Sub(ctx context.Context, req *api.SubRequest, callOptions ...callopt.Option) (r *api.SubResponse, err error) {
	ctx = client.NewCtxWithCallOptions(ctx, callOptions)
	return p.kClient.Sub(ctx, req)
}

func (p *kZkServer_OperationsClient) ConStartGetBroker(ctx context.Context, req *api.ConStartGetBrokRequest, callOptions ...callopt.Option) (r *api.ConStartGetBrokResponse, err error) {
	ctx = client.NewCtxWithCallOptions(ctx, callOptions)
	return p.kClient.ConStartGetBroker(ctx, req)
}

func (p *kZkServer_OperationsClient) BroInfo(ctx context.Context, req *api.BroInfoRequest, callOptions ...callopt.Option) (r *api.BroInfoResponse, err error) {
	ctx = client.NewCtxWithCallOptions(ctx, callOptions)
	return p.kClient.BroInfo(ctx, req)
}

func (p *kZkServer_OperationsClient) UpdateRep(ctx context.Context, req *api.UpdateRepRequest, callOptions ...callopt.Option) (r *api.UpdateRepResponse, err error) {
	ctx = client.NewCtxWithCallOptions(ctx, callOptions)
	return p.kClient.UpdateRep(ctx, req)
}

func (p *kZkServer_OperationsClient) UpdatePTPOffset(ctx context.Context, req *api.UpdatePTPOffsetRequest, callOptions ...callopt.Option) (r *api.UpdatePTPOffsetResponse, err error) {
	ctx = client.NewCtxWithCallOptions(ctx, callOptions)
	return p.kClient.UpdatePTPOffset(ctx, req)
}
