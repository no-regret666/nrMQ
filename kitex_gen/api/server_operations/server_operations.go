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
	"StartToGet": kitex.NewMethodInfo(
		startToGetHandler,
		newServer_OperationsStartToGetArgs,
		newServer_OperationsStartToGetResult,
		false,
		kitex.WithStreamingMode(kitex.StreamingNone),
	),
	"Pull": kitex.NewMethodInfo(
		pullHandler,
		newServer_OperationsPullArgs,
		newServer_OperationsPullResult,
		false,
		kitex.WithStreamingMode(kitex.StreamingNone),
	),
	"PrepareAccept": kitex.NewMethodInfo(
		prepareAcceptHandler,
		newServer_OperationsPrepareAcceptArgs,
		newServer_OperationsPrepareAcceptResult,
		false,
		kitex.WithStreamingMode(kitex.StreamingNone),
	),
	"CloseAccept": kitex.NewMethodInfo(
		closeAcceptHandler,
		newServer_OperationsCloseAcceptArgs,
		newServer_OperationsCloseAcceptResult,
		false,
		kitex.WithStreamingMode(kitex.StreamingNone),
	),
	"PrepareState": kitex.NewMethodInfo(
		prepareStateHandler,
		newServer_OperationsPrepareStateArgs,
		newServer_OperationsPrepareStateResult,
		false,
		kitex.WithStreamingMode(kitex.StreamingNone),
	),
	"PrepareSend": kitex.NewMethodInfo(
		prepareSendHandler,
		newServer_OperationsPrepareSendArgs,
		newServer_OperationsPrepareSendResult,
		false,
		kitex.WithStreamingMode(kitex.StreamingNone),
	),
	"AddRaftPartition": kitex.NewMethodInfo(
		addRaftPartitionHandler,
		newServer_OperationsAddRaftPartitionArgs,
		newServer_OperationsAddRaftPartitionResult,
		false,
		kitex.WithStreamingMode(kitex.StreamingNone),
	),
	"CloseRaftPartition": kitex.NewMethodInfo(
		closeRaftPartitionHandler,
		newServer_OperationsCloseRaftPartitionArgs,
		newServer_OperationsCloseRaftPartitionResult,
		false,
		kitex.WithStreamingMode(kitex.StreamingNone),
	),
	"AddFetchPartition": kitex.NewMethodInfo(
		addFetchPartitionHandler,
		newServer_OperationsAddFetchPartitionArgs,
		newServer_OperationsAddFetchPartitionResult,
		false,
		kitex.WithStreamingMode(kitex.StreamingNone),
	),
	"CloseFetchPartition": kitex.NewMethodInfo(
		closeFetchPartitionHandler,
		newServer_OperationsCloseFetchPartitionArgs,
		newServer_OperationsCloseFetchPartitionResult,
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

func startToGetHandler(ctx context.Context, handler interface{}, arg, result interface{}) error {
	realArg := arg.(*api.Server_OperationsStartToGetArgs)
	realResult := result.(*api.Server_OperationsStartToGetResult)
	success, err := handler.(api.Server_Operations).StartToGet(ctx, realArg.Req)
	if err != nil {
		return err
	}
	realResult.Success = success
	return nil
}
func newServer_OperationsStartToGetArgs() interface{} {
	return api.NewServer_OperationsStartToGetArgs()
}

func newServer_OperationsStartToGetResult() interface{} {
	return api.NewServer_OperationsStartToGetResult()
}

func pullHandler(ctx context.Context, handler interface{}, arg, result interface{}) error {
	realArg := arg.(*api.Server_OperationsPullArgs)
	realResult := result.(*api.Server_OperationsPullResult)
	success, err := handler.(api.Server_Operations).Pull(ctx, realArg.Req)
	if err != nil {
		return err
	}
	realResult.Success = success
	return nil
}
func newServer_OperationsPullArgs() interface{} {
	return api.NewServer_OperationsPullArgs()
}

func newServer_OperationsPullResult() interface{} {
	return api.NewServer_OperationsPullResult()
}

func prepareAcceptHandler(ctx context.Context, handler interface{}, arg, result interface{}) error {
	realArg := arg.(*api.Server_OperationsPrepareAcceptArgs)
	realResult := result.(*api.Server_OperationsPrepareAcceptResult)
	success, err := handler.(api.Server_Operations).PrepareAccept(ctx, realArg.Req)
	if err != nil {
		return err
	}
	realResult.Success = success
	return nil
}
func newServer_OperationsPrepareAcceptArgs() interface{} {
	return api.NewServer_OperationsPrepareAcceptArgs()
}

func newServer_OperationsPrepareAcceptResult() interface{} {
	return api.NewServer_OperationsPrepareAcceptResult()
}

func closeAcceptHandler(ctx context.Context, handler interface{}, arg, result interface{}) error {
	realArg := arg.(*api.Server_OperationsCloseAcceptArgs)
	realResult := result.(*api.Server_OperationsCloseAcceptResult)
	success, err := handler.(api.Server_Operations).CloseAccept(ctx, realArg.Req)
	if err != nil {
		return err
	}
	realResult.Success = success
	return nil
}
func newServer_OperationsCloseAcceptArgs() interface{} {
	return api.NewServer_OperationsCloseAcceptArgs()
}

func newServer_OperationsCloseAcceptResult() interface{} {
	return api.NewServer_OperationsCloseAcceptResult()
}

func prepareStateHandler(ctx context.Context, handler interface{}, arg, result interface{}) error {
	realArg := arg.(*api.Server_OperationsPrepareStateArgs)
	realResult := result.(*api.Server_OperationsPrepareStateResult)
	success, err := handler.(api.Server_Operations).PrepareState(ctx, realArg.Req)
	if err != nil {
		return err
	}
	realResult.Success = success
	return nil
}
func newServer_OperationsPrepareStateArgs() interface{} {
	return api.NewServer_OperationsPrepareStateArgs()
}

func newServer_OperationsPrepareStateResult() interface{} {
	return api.NewServer_OperationsPrepareStateResult()
}

func prepareSendHandler(ctx context.Context, handler interface{}, arg, result interface{}) error {
	realArg := arg.(*api.Server_OperationsPrepareSendArgs)
	realResult := result.(*api.Server_OperationsPrepareSendResult)
	success, err := handler.(api.Server_Operations).PrepareSend(ctx, realArg.Req)
	if err != nil {
		return err
	}
	realResult.Success = success
	return nil
}
func newServer_OperationsPrepareSendArgs() interface{} {
	return api.NewServer_OperationsPrepareSendArgs()
}

func newServer_OperationsPrepareSendResult() interface{} {
	return api.NewServer_OperationsPrepareSendResult()
}

func addRaftPartitionHandler(ctx context.Context, handler interface{}, arg, result interface{}) error {
	realArg := arg.(*api.Server_OperationsAddRaftPartitionArgs)
	realResult := result.(*api.Server_OperationsAddRaftPartitionResult)
	success, err := handler.(api.Server_Operations).AddRaftPartition(ctx, realArg.Req)
	if err != nil {
		return err
	}
	realResult.Success = success
	return nil
}
func newServer_OperationsAddRaftPartitionArgs() interface{} {
	return api.NewServer_OperationsAddRaftPartitionArgs()
}

func newServer_OperationsAddRaftPartitionResult() interface{} {
	return api.NewServer_OperationsAddRaftPartitionResult()
}

func closeRaftPartitionHandler(ctx context.Context, handler interface{}, arg, result interface{}) error {
	realArg := arg.(*api.Server_OperationsCloseRaftPartitionArgs)
	realResult := result.(*api.Server_OperationsCloseRaftPartitionResult)
	success, err := handler.(api.Server_Operations).CloseRaftPartition(ctx, realArg.Req)
	if err != nil {
		return err
	}
	realResult.Success = success
	return nil
}
func newServer_OperationsCloseRaftPartitionArgs() interface{} {
	return api.NewServer_OperationsCloseRaftPartitionArgs()
}

func newServer_OperationsCloseRaftPartitionResult() interface{} {
	return api.NewServer_OperationsCloseRaftPartitionResult()
}

func addFetchPartitionHandler(ctx context.Context, handler interface{}, arg, result interface{}) error {
	realArg := arg.(*api.Server_OperationsAddFetchPartitionArgs)
	realResult := result.(*api.Server_OperationsAddFetchPartitionResult)
	success, err := handler.(api.Server_Operations).AddFetchPartition(ctx, realArg.Req)
	if err != nil {
		return err
	}
	realResult.Success = success
	return nil
}
func newServer_OperationsAddFetchPartitionArgs() interface{} {
	return api.NewServer_OperationsAddFetchPartitionArgs()
}

func newServer_OperationsAddFetchPartitionResult() interface{} {
	return api.NewServer_OperationsAddFetchPartitionResult()
}

func closeFetchPartitionHandler(ctx context.Context, handler interface{}, arg, result interface{}) error {
	realArg := arg.(*api.Server_OperationsCloseFetchPartitionArgs)
	realResult := result.(*api.Server_OperationsCloseFetchPartitionResult)
	success, err := handler.(api.Server_Operations).CloseFetchPartition(ctx, realArg.Req)
	if err != nil {
		return err
	}
	realResult.Success = success
	return nil
}
func newServer_OperationsCloseFetchPartitionArgs() interface{} {
	return api.NewServer_OperationsCloseFetchPartitionArgs()
}

func newServer_OperationsCloseFetchPartitionResult() interface{} {
	return api.NewServer_OperationsCloseFetchPartitionResult()
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

func (p *kClient) StartToGet(ctx context.Context, req *api.InfoGetRequest) (r *api.InfoGetResponse, err error) {
	var _args api.Server_OperationsStartToGetArgs
	_args.Req = req
	var _result api.Server_OperationsStartToGetResult
	if err = p.c.Call(ctx, "StartToGet", &_args, &_result); err != nil {
		return
	}
	return _result.GetSuccess(), nil
}

func (p *kClient) Pull(ctx context.Context, req *api.PullRequest) (r *api.PullResponse, err error) {
	var _args api.Server_OperationsPullArgs
	_args.Req = req
	var _result api.Server_OperationsPullResult
	if err = p.c.Call(ctx, "Pull", &_args, &_result); err != nil {
		return
	}
	return _result.GetSuccess(), nil
}

func (p *kClient) PrepareAccept(ctx context.Context, req *api.PrepareAcceptRequest) (r *api.PrepareAcceptResponse, err error) {
	var _args api.Server_OperationsPrepareAcceptArgs
	_args.Req = req
	var _result api.Server_OperationsPrepareAcceptResult
	if err = p.c.Call(ctx, "PrepareAccept", &_args, &_result); err != nil {
		return
	}
	return _result.GetSuccess(), nil
}

func (p *kClient) CloseAccept(ctx context.Context, req *api.CloseAcceptRequest) (r *api.CloseAcceptResponse, err error) {
	var _args api.Server_OperationsCloseAcceptArgs
	_args.Req = req
	var _result api.Server_OperationsCloseAcceptResult
	if err = p.c.Call(ctx, "CloseAccept", &_args, &_result); err != nil {
		return
	}
	return _result.GetSuccess(), nil
}

func (p *kClient) PrepareState(ctx context.Context, req *api.PrepareStateRequest) (r *api.PrepareStateResponse, err error) {
	var _args api.Server_OperationsPrepareStateArgs
	_args.Req = req
	var _result api.Server_OperationsPrepareStateResult
	if err = p.c.Call(ctx, "PrepareState", &_args, &_result); err != nil {
		return
	}
	return _result.GetSuccess(), nil
}

func (p *kClient) PrepareSend(ctx context.Context, req *api.PrepareSendRequest) (r *api.PrepareSendResponse, err error) {
	var _args api.Server_OperationsPrepareSendArgs
	_args.Req = req
	var _result api.Server_OperationsPrepareSendResult
	if err = p.c.Call(ctx, "PrepareSend", &_args, &_result); err != nil {
		return
	}
	return _result.GetSuccess(), nil
}

func (p *kClient) AddRaftPartition(ctx context.Context, req *api.AddRaftPartitionRequest) (r *api.AddRaftPartitionResponse, err error) {
	var _args api.Server_OperationsAddRaftPartitionArgs
	_args.Req = req
	var _result api.Server_OperationsAddRaftPartitionResult
	if err = p.c.Call(ctx, "AddRaftPartition", &_args, &_result); err != nil {
		return
	}
	return _result.GetSuccess(), nil
}

func (p *kClient) CloseRaftPartition(ctx context.Context, req *api.CloseAcceptRequest) (r *api.CloseRaftPartitionResponse, err error) {
	var _args api.Server_OperationsCloseRaftPartitionArgs
	_args.Req = req
	var _result api.Server_OperationsCloseRaftPartitionResult
	if err = p.c.Call(ctx, "CloseRaftPartition", &_args, &_result); err != nil {
		return
	}
	return _result.GetSuccess(), nil
}

func (p *kClient) AddFetchPartition(ctx context.Context, req *api.AddFetchPartitionRequest) (r *api.AddFetchPartitionResponse, err error) {
	var _args api.Server_OperationsAddFetchPartitionArgs
	_args.Req = req
	var _result api.Server_OperationsAddFetchPartitionResult
	if err = p.c.Call(ctx, "AddFetchPartition", &_args, &_result); err != nil {
		return
	}
	return _result.GetSuccess(), nil
}

func (p *kClient) CloseFetchPartition(ctx context.Context, req *api.CloseAcceptRequest) (r *api.CloseFetchPartitionResponse, err error) {
	var _args api.Server_OperationsCloseFetchPartitionArgs
	_args.Req = req
	var _result api.Server_OperationsCloseFetchPartitionResult
	if err = p.c.Call(ctx, "CloseFetchPartition", &_args, &_result); err != nil {
		return
	}
	return _result.GetSuccess(), nil
}
