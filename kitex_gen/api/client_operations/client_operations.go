// Code generated by Kitex v0.12.1. DO NOT EDIT.

package client_operations

import (
	"context"
	"errors"
	client "github.com/cloudwego/kitex/client"
	kitex "github.com/cloudwego/kitex/pkg/serviceinfo"
	api "nrMQ/kitex_gen/api"
)

var errInvalidMessageType = errors.New("invalid message type for service method handler")

var serviceMethods = map[string]kitex.MethodInfo{
	"pub": kitex.NewMethodInfo(
		pubHandler,
		newClient_OperationsPubArgs,
		newClient_OperationsPubResult,
		false,
		kitex.WithStreamingMode(kitex.StreamingNone),
	),
	"pingpong": kitex.NewMethodInfo(
		pingpongHandler,
		newClient_OperationsPingpongArgs,
		newClient_OperationsPingpongResult,
		false,
		kitex.WithStreamingMode(kitex.StreamingNone),
	),
}

var (
	client_OperationsServiceInfo                = NewServiceInfo()
	client_OperationsServiceInfoForClient       = NewServiceInfoForClient()
	client_OperationsServiceInfoForStreamClient = NewServiceInfoForStreamClient()
)

// for server
func serviceInfo() *kitex.ServiceInfo {
	return client_OperationsServiceInfo
}

// for stream client
func serviceInfoForStreamClient() *kitex.ServiceInfo {
	return client_OperationsServiceInfoForStreamClient
}

// for client
func serviceInfoForClient() *kitex.ServiceInfo {
	return client_OperationsServiceInfoForClient
}

// NewServiceInfo creates a new ServiceInfo containing all methods
func NewServiceInfo() *kitex.ServiceInfo {
	return newServiceInfo(false, true, true)
}

// NewServiceInfo creates a new ServiceInfo containing non-streaming methods
func NewServiceInfoForClient() *kitex.ServiceInfo {
	return newServiceInfo(false, false, true)
}
func NewServiceInfoForStreamClient() *kitex.ServiceInfo {
	return newServiceInfo(true, true, false)
}

func newServiceInfo(hasStreaming bool, keepStreamingMethods bool, keepNonStreamingMethods bool) *kitex.ServiceInfo {
	serviceName := "Client_Operations"
	handlerType := (*api.Client_Operations)(nil)
	methods := map[string]kitex.MethodInfo{}
	for name, m := range serviceMethods {
		if m.IsStreaming() && !keepStreamingMethods {
			continue
		}
		if !m.IsStreaming() && !keepNonStreamingMethods {
			continue
		}
		methods[name] = m
	}
	extra := map[string]interface{}{
		"PackageName": "api",
	}
	if hasStreaming {
		extra["streaming"] = hasStreaming
	}
	svcInfo := &kitex.ServiceInfo{
		ServiceName:     serviceName,
		HandlerType:     handlerType,
		Methods:         methods,
		PayloadCodec:    kitex.Thrift,
		KiteXGenVersion: "v0.12.1",
		Extra:           extra,
	}
	return svcInfo
}

func pubHandler(ctx context.Context, handler interface{}, arg, result interface{}) error {
	realArg := arg.(*api.Client_OperationsPubArgs)
	realResult := result.(*api.Client_OperationsPubResult)
	success, err := handler.(api.Client_Operations).Pub(ctx, realArg.Req)
	if err != nil {
		return err
	}
	realResult.Success = success
	return nil
}
func newClient_OperationsPubArgs() interface{} {
	return api.NewClient_OperationsPubArgs()
}

func newClient_OperationsPubResult() interface{} {
	return api.NewClient_OperationsPubResult()
}

func pingpongHandler(ctx context.Context, handler interface{}, arg, result interface{}) error {
	realArg := arg.(*api.Client_OperationsPingpongArgs)
	realResult := result.(*api.Client_OperationsPingpongResult)
	success, err := handler.(api.Client_Operations).Pingpong(ctx, realArg.Req)
	if err != nil {
		return err
	}
	realResult.Success = success
	return nil
}
func newClient_OperationsPingpongArgs() interface{} {
	return api.NewClient_OperationsPingpongArgs()
}

func newClient_OperationsPingpongResult() interface{} {
	return api.NewClient_OperationsPingpongResult()
}

type kClient struct {
	c client.Client
}

func newServiceClient(c client.Client) *kClient {
	return &kClient{
		c: c,
	}
}

func (p *kClient) Pub(ctx context.Context, req *api.PubRequest) (r *api.PubResponse, err error) {
	var _args api.Client_OperationsPubArgs
	_args.Req = req
	var _result api.Client_OperationsPubResult
	if err = p.c.Call(ctx, "pub", &_args, &_result); err != nil {
		return
	}
	return _result.GetSuccess(), nil
}

func (p *kClient) Pingpong(ctx context.Context, req *api.PingPongRequest) (r *api.PingPongResponse, err error) {
	var _args api.Client_OperationsPingpongArgs
	_args.Req = req
	var _result api.Client_OperationsPingpongResult
	if err = p.c.Call(ctx, "pingpong", &_args, &_result); err != nil {
		return
	}
	return _result.GetSuccess(), nil
}
