package server

import (
	"context"
	"fmt"
	"github.com/cloudwego/kitex/server"
	"nrMQ/kitex_gen/api"
	"nrMQ/kitex_gen/api/server_operations"
	"nrMQ/kitex_gen/api/zkserver_operations"
	"nrMQ/logger"
	"nrMQ/zookeeper"
)

// RPCServer 可以注册两种server
// 一个是客户端（生产者和消费者连接的模式，负责订阅等请求和对zookeeper的请求）使用的server
// 另一个是Broker获取zookeeper信息和调度各个Broker的调度者
type RPCServer struct {
	srv_cli  *server.Server
	srv_bro  *server.Server
	zkinfo   zookeeper.ZkInfo
	server   *Server
	zkserver *ZKServer
}

func (s *RPCServer) Start(opts_cli, opts_raf, opts_zks []server.Option, opt Options) error {
	switch opt.Tag {
	case BROKER:
		s.server = NewServer(s.zkinfo)
		s.server.Make(opt, opts_raf)

		srv_cli_bro := server_operations.NewServer(s, opts_cli...)
		s.srv_cli = &srv_cli_bro
		logger.DEBUG(logger.DLog, "%v the raft %v start rpcserver for clients\n", opt.Name, opt.Me)
		go func() {
			err := srv_cli_bro.Run()

			if err != nil {
				fmt.Println(err.Error())
			}
		}()

	case ZKBROKER:
		s.zkserver = NewZKServer(s.zkinfo)
		s.zkserver.make(opt)

		srv_bro_cli := zkserver_operations.NewServer(s, opts_zks...)
		s.srv_bro = &srv_bro_cli
		logger.DEBUG(logger.DLog, "ZkServer start rpcserver for brokers\n")
		go func() {
			err := srv_bro_cli.Run()

			if err != nil {
				fmt.Println(err.Error())
			}
		}()
	}

	return nil
}

func (s *RPCServer) Push(ctx context.Context, req *api.PushRequest) (r *api.PushResponse, err error) {
	//TODO implement me
	panic("implement me")
}

func (s *RPCServer) ConInfo(ctx context.Context, req *api.InfoRequest) (r *api.InfoResponse, err error) {
	//TODO implement me
	panic("implement me")
}

func (s *RPCServer) StartToGet(ctx context.Context, req *api.InfoGetRequest) (r *api.InfoGetResponse, err error) {
	//TODO implement me
	panic("implement me")
}

func (s *RPCServer) Pull(ctx context.Context, req *api.PullRequest) (r *api.PullResponse, err error) {
	//TODO implement me
	panic("implement me")
}

func (s *RPCServer) CreateTopic(ctx context.Context, req *api.CreateTopicRequest) (r *api.CreateTopicResponse, err error) {
	//TODO implement me
	panic("implement me")
}

func (s *RPCServer) CreatePart(ctx context.Context, req *api.CreatePartRequest) (r *api.CreatePartResponse, err error) {
	//TODO implement me
	panic("implement me")
}

func (s *RPCServer) ProGetBroker(ctx context.Context, req *api.ProGetBrokRequest) (r *api.ProGetBrokResponse, err error) {
	//TODO implement me
	panic("implement me")
}

func (s *RPCServer) SetPartitionState(ctx context.Context, req *api.SetPartitionStateRequest) (r *api.SetPartitionStateResponse, err error) {
	//TODO implement me
	panic("implement me")
}

func (s *RPCServer) Sub(ctx context.Context, req *api.SubRequest) (r *api.SubResponse, err error) {
	//TODO implement me
	panic("implement me")
}

func (s *RPCServer) ConStartGetBroker(ctx context.Context, req *api.ConStartGetBrokRequest) (r *api.ConStartGetBrokResponse, err error) {
	//TODO implement me
	panic("implement me")
}

func NewRPCServer(zkinfo zookeeper.ZkInfo) RPCServer {
	logger.LOGinit()
	return RPCServer{
		zkinfo: zkinfo,
	}
}

// zkserver---->broker server
// 通知broker准备接收生产者信息
func (s *RPCServer) PrepareAccept(ctx context.Context, req *api.PrepareAcceptRequest) (r *api.PrepareAcceptResponse, err error) {

}
