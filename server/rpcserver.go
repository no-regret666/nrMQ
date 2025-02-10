package server

import (
	"context"
	"github.com/cloudwego/kitex/server"
	"nrMQ/kitex_gen/api"
	"nrMQ/logger"
	"nrMQ/zookeeper"
)

// RPCServer 可以注册两种server
// 一个是客户端（生产者和消费者连接的模式，负责订阅等请求和对zookeeper的请求）使用的server
// 另一个是Broker获取zookeeper信息和调度各个Broker的调度者
type RPCServer struct {
	srv_cli  *server.Server
	srv_bro  *server.Server
	zkinfo   zookeeper.ZKInfo
	server   *Server
	zkserver *ZKServer
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
