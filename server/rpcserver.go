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

func NewRPCServer(zkinfo zookeeper.ZkInfo) RPCServer {
	logger.LOGinit()
	return RPCServer{
		zkinfo: zkinfo,
	}
}

func (s *RPCServer) Start(opts_cli, opts_zks, opts_raf []server.Option, opt Options) error {
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

func (s *RPCServer) ShutDown_server() {
	if s.srv_bro != nil {
		(*s.srv_bro).Stop()
	}
	if s.zkserver != nil {
		(*s.srv_cli).Stop()
	}
}

// producer---->broker server
func (s *RPCServer) Push(ctx context.Context, req *api.PushRequest) (r *api.PushResponse, err error) {
	ret, err := s.server.PushHandle(info{
		producer:   req.Producer,
		topic_name: req.Topic,
		part_name:  req.Key,
		ack:        req.Ack,
		cmdindex:   req.CmdIndex,
		message:    req.Message,
		size:       req.Size,
	})

	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}

	return &api.PushResponse{
		Ret: true,
		Err: ret,
	}, nil
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

// producer--->zkserver
// 先在zookeeper上创建一个Topic,当生产该信息时，或消费信息时再有zkserver发送信息到broker让broker创建
func (s *RPCServer) CreateTopic(ctx context.Context, req *api.CreateTopicRequest) (r *api.CreateTopicResponse, err error) {
	info := s.zkserver.CreateTopic(Info_in{
		topic_name: req.TopicName,
	})

	if info.Err != nil {
		return &api.CreateTopicResponse{
			Ret: false,
			Err: info.Err.Error(),
		}, info.Err
	}

	return &api.CreateTopicResponse{
		Ret: true,
		Err: "ok",
	}, nil
}

// producer--->zkserver
// 先在zookeeper上创建一个Partition,当生产该信息时，或消费信息时再有zkserver发送信息到broker让broker创建
func (s *RPCServer) CreatePart(ctx context.Context, req *api.CreatePartRequest) (r *api.CreatePartResponse, err error) {
	info := s.zkserver.CreatePart(Info_in{
		topic_name: req.TopicName,
		part_name:  req.PartName,
	})

	if info.Err != nil {
		return &api.CreatePartResponse{
			Ret: false,
			Err: info.Err.Error(),
		}, info.Err
	}

	return &api.CreatePartResponse{
		Ret: true,
		Err: "ok",
	}, nil
}

// producer获取该向哪个broker发送消息
func (s *RPCServer) ProGetBroker(ctx context.Context, req *api.ProGetBrokRequest) (r *api.ProGetBrokResponse, err error) {
	info := s.zkserver.ProGetBroker(Info_in{
		topic_name: req.TopicName,
		part_name:  req.PartName,
	})

	if info.Err != nil {
		return &api.ProGetBrokResponse{
			Ret: false,
		}, info.Err
	}

	return &api.ProGetBrokResponse{
		Ret:            true,
		BrokerHostPort: info.bro_host_port,
	}, nil
}

func (s *RPCServer) SetPartitionState(ctx context.Context, req *api.SetPartitionStateRequest) (r *api.SetPartitionStateResponse, err error) {
	info := s.zkserver.SetPartitionState(Info_in{
		topic_name: req.Topic,
		part_name:  req.Partition,
		option:     req.Option,
		dupnum:     req.Dupnum,
	})

	if info.Err != nil {
		return &api.SetPartitionStateResponse{
			Ret: false,
			Err: info.Err.Error(),
		}, info.Err
	}

	return &api.SetPartitionStateResponse{
		Ret: true,
		Err: "ok",
	}, nil
}

func (s *RPCServer) Sub(ctx context.Context, req *api.SubRequest) (r *api.SubResponse, err error) {
	err = s.zkserver.SubHandle(Info_in{
		cli_name:   req.Consumer,
		topic_name: req.Topic,
		part_name:  req.Key,
		option:     req.Option,
	})

	if err == nil {
		return &api.SubResponse{
			Ret: true,
		}, nil
	}

	return &api.SubResponse{Ret: false}, err
}

func (s *RPCServer) ConStartGetBroker(ctx context.Context, req *api.ConStartGetBrokRequest) (r *api.ConStartGetBrokResponse, err error) {
	parts, size, err := s.zkserver.HandStartGetBroker(Info_in{
		cli_name:   req.CliName,
		topic_name: req.TopicName,
		part_name:  req.PartName,
		option:     req.Option,
		index:      req.Index,
	})
	if err != nil {
		return &api.ConStartGetBrokResponse{
			Ret: false,
		}, err
	}

	return &api.ConStartGetBrokResponse{
		Ret:   true,
		Size:  int64(size),
		Parts: parts,
	}, nil
}

// zkserver---->broker server
// 通知broker准备接收生产者信息
func (s *RPCServer) PrepareAccept(ctx context.Context, req *api.PrepareAcceptRequest) (r *api.PrepareAcceptResponse, err error) {
	ret, err := s.server.PrepareAcceptHandle(info{
		topic_name: req.TopicName,
		part_name:  req.PartName,
		file_name:  req.FileName,
	})
	if err != nil {
		return &api.PrepareAcceptResponse{
			Ret: false,
			Err: ret,
		}, err
	}

	return &api.PrepareAcceptResponse{
		Ret: true,
		Err: ret,
	}, nil
}

func (s *RPCServer) CloseAccept(ctx context.Context, req *api.CloseAcceptRequest) (r *api.CloseAcceptResponse, err error) {
	//TODO implement me
	panic("implement me")
}

func (s *RPCServer) PrepareState(ctx context.Context, req *api.PrepareStateRequest) (r *api.PrepareStateResponse, err error) {
	//TODO implement me
	panic("implement me")
}

func (s *RPCServer) PrepareSend(ctx context.Context, req *api.PrepareSendRequest) (r *api.PrepareSendResponse, err error) {
	//TODO implement me
	panic("implement me")
}

func (s *RPCServer) BroInfo(ctx context.Context, req *api.BroInfoRequest) (r *api.BroInfoResponse, err error) {
	//TODO implement me
	panic("implement me")
}
