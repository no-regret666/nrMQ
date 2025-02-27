package server

import (
	"context"
	"errors"
	"github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/server"
	"nrMQ/kitex_gen/api"
	"nrMQ/kitex_gen/api/server_operations"
	"nrMQ/kitex_gen/api/zkserver_operations"
	"nrMQ/kitex_gen/raftoperations/raft_operations"
	"nrMQ/logger"
	"nrMQ/zookeeper"
	"sync"
)

const (
	NODE_SIZE = 24
)

type Server struct {
	Name     string
	me       int
	zk       *zookeeper.ZK
	zkclient zkserver_operations.Client
	mu       sync.RWMutex

	aplych    chan info
	topics    map[string]*Topic
	consumers map[string]*Client
	brokers   map[string]*raft_operations.Client

	//raft
	pafts_rafts *parts_raft

	//fetch
	parts_fetch   map[string]string                    //topicName + partitionName to broker HostPort
	brokers_fetch map[string]*server_operations.Client //brokerName to Client
}

type Key struct {
	Size        int64 `json:"size"`
	Start_index int64 `json:"start_index"`
	End_index   int64 `json:"end_index"`
}

type Message struct {
	Index      int64  `json:"index"`
	Size       int8   `json:"size"`
	Topic_name string `json:"topic_name"`
	Part_name  string `json:"part_name"`
	Msg        []byte `json:"msg"`
}

type info struct {
	topicName string
	partName  string
	fileName  string
	newName   string
	option    int8
	offset    int64
	size      int8

	ack int8

	producer string
	consumer string
	message  []byte

	//raft
	brokers map[string]string
	bro_me  map[string]int
	me      int

	//fetch
	LeaderBroker string
	HostPort     string

	//update dup
	zkclient   *zkserver_operations.Client
	BrokerName string
}

func NewServer(zkinfo zookeeper.ZkInfo) *Server {
	return &Server{
		zk: zookeeper.NewZK(zkinfo),
		mu: sync.RWMutex{},
	}
}

func (s *Server) Make(opt Options, opt_cli []server.Option) {
	s.consumers = make(map[string]*Client)
	s.topics = make(map[string]*Topic)
	s.brokers = make(map[string]*raft_operations.Client)
	s.parts_fetch = make(map[string]string)
	s.brokers_fetch = make(map[string]*server_operations.Client)
	s.aplych = make(chan info)

	s.CheckList()
	s.Name = opt.Name
	s.me = opt.Me

	//本地创建parts-raft，为raft同步做准备

	//在zookeeper上创建一个永久节点，若存在则不需要创建
	err := s.zk.RegisterNode(zookeeper.BrokerNode{
		Name:         s.Name,
		Me:           s.me,
		BrokHostPort: opt.Broker_Host_Port,
		RaftHostPort: opt.Raft_Host_Port,
	})
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}

	//创建临时节点，用于zkserver的watch
	err = s.zk.CreateState(s.Name)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}

	//连接zkServer，并将自己的info发送到zkServer
	zkclient, err := zkserver_operations.NewClient(opt.Name, client.WithHostPorts(opt.ZKServer_Host_Port))
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}
	s.zkclient = zkclient

	resp, err := zkclient.BroInfo(context.Background(), &api.BroInfoRequest{
		BrokerName:     opt.Name,
		BrokerHostPort: opt.Broker_Host_Port,
	})
	if err != nil || !resp.Ret {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}

	//开始获取管道中的内容，写入文件或更新leader

}

// 准备接收信息
// 检查topic和partition是否存在，不存在则需要创建
// 设置partition中的file和fd,start_index等信息
func (s *Server) PrepareAcceptHandle(in info) (ret string, err error) {
	//检查topic
	s.mu.Lock()
	topic, ok := s.topics[in.topicName]
	if !ok {
		topic = NewTopic(s.Name, in.topicName)
		s.topics[in.topicName] = topic
	}

	s.mu.Unlock()

	//检查partition
	return topic.PrepareAcceptHandle(in)
}

// start到该partition中的raft集群中
// 收到返回后判断该写入还是返回
func (s *Server) PushHandle(in info) (ret string, err error) {
	logger.DEBUG(logger.DLog, "get Message from producer\n")
	s.mu.RLock()
	topic, ok := s.topics[in.topic_name]
	part_raft := s.pafts_rafts
	s.mu.RUnlock()

	if !ok {
		ret = "this topic is not in this broker"
		logger.DEBUG(logger.DError, "Topic %v,is not in this broker", in.topic_name)
		return ret, errors.New(ret)
	}

	switch in.ack {
	case -1: //raft同步，并写入
		ret, err = part_raft.Append(in)
	case 1: //leader写入，不等待同步
		err = topic.addMessage(in)
	case 0: //直接返回
		go topic.addMessage(in)
	}

	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return err.Error(), err
	}

	return ret, err
}

// PullHandle
// Pull message
func (s *Server) PullHandle(in info) (MSGS, error) {
	//若该请求属于PTP则读取index，获取上次的index，写入zookeeper中
	logger.DEBUG(logger.DLog, "%v get pull request the in.op(%v) TOP_PTP_PULL(%v)\n", s.Name, in.option, TOPIC_NIL_PTP_PULL)
	if in.option == TOPIC_NIL_PTP_PULL {

	}
}
