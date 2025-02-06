// Code generated by Kitex v0.12.1. DO NOT EDIT.

package server_operations

import (
	"context"
	"errors"
	client "github.com/cloudwego/kitex/client"
	kitex "github.com/cloudwego/kitex/pkg/serviceinfo"
	api "nrMQ/kitex_gen/api"
)

var errInvalidMessageType = errors.New("invalid message type for service method handler")

var serviceMethods = map[string]kitex.MethodInfo{
	"push": kitex.NewMethodInfo(
		pushHandler,
		newServer_OperationsPushArgs,
		newServer_OperationsPushResult,
		false,
		kitex.WithStreamingMode(kitex.StreamingNone),
	),
	"ConInfo": kitex.NewMethodInfo(
		conInfoHandler,
		newServer_OperationsConInfoArgs,
		newServer_OperationsConInfoResult,
		false,
		kitex.WithStreamingMode(kitex.StreamingNone),
	),
}

var (
	server_OperationsServiceInfo                = NewServiceInfo()
	server_OperationsServiceInfoForClient       = NewServiceInfoForClient()
	server_OperationsServiceInfoForStreamClient = NewServiceInfoForStreamClient()
)

// for server
func serviceInfo() *kitex.ServiceInfo {
	return server_OperationsServiceInfo
}

// for stream client
func serviceInfoForStreamClient() *kitex.ServiceInfo {
	return server_OperationsServiceInfoForStreamClient
}

// for client
func serviceInfoForClient() *kitex.ServiceInfo {
	return server_OperationsServiceInfoForClient
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
	serviceName := "Server_Operations"
	handlerType := (*api.Server_Operations)(nil)
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

func pushHandler(ctx context.Context, handler interface{}, arg, result interface{}) error {
	realArg := arg.(*api.Server_OperationsPushArgs)
	realResult := result.(*api.Server_OperationsPushResult)
	success, err := handler.(api.Server_Operations).Push(ctx, realArg.Req)
	if err != nil {
		return err
	}
	realResult.Success = success
	return nil
}
func newServer_OperationsPushArgs() interface{} {
	return api.NewServer_OperationsPushArgs()
}

func newServer_OperationsPushResult() interface{} {
	return api.NewServer_OperationsPushResult()
}

func conInfoHandler(ctx context.Context, handler interface{}, arg, result interface{}) error {
	realArg := arg.(*api.Server_OperationsConInfoArgs)
	realResult := result.(*api.Server_OperationsConInfoResult)
	success, err := handler.(api.Server_Operations).ConInfo(ctx, realArg.Req)
	if err != nil {
		return err
	}
	realResult.Success = success
	return nil
}
func newServer_OperationsConInfoArgs() interface{} {
	return api.NewServer_OperationsConInfoArgs()
}

func newServer_OperationsConInfoResult() interface{} {
	return api.NewServer_OperationsConInfoResult()
}

type kClient struct {
	c client.Client
}

func newServiceClient(c client.Client) *kClient {
	return &kClient{
		c: c,
	}
}

func (p *kClient) Push(ctx context.Context, req *api.PushRequest) (r *api.PushResponse, err error) {
	var _args api.Server_OperationsPushArgs
	_args.Req = req
	var _result api.Server_OperationsPushResult
	if err = p.c.Call(ctx, "push", &_args, &_result); err != nil {
		return
	}
	return _result.GetSuccess(), nil
}

func (p *kClient) ConInfo(ctx context.Context, req *api.InfoRequest) (r *api.InfoResponse, err error) {
	var _args api.Server_OperationsConInfoArgs
	_args.Req = req
	var _result api.Server_OperationsConInfoResult
	if err = p.c.Call(ctx, "ConInfo", &_args, &_result); err != nil {
		return
	}
	return _result.GetSuccess(), nil
}
