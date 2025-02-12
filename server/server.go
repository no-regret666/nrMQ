package server

import (
	"github.com/cloudwego/kitex/server"
	"nrMQ/kitex_gen/api/server_operations"
	"nrMQ/kitex_gen/api/zkserver_operation"
	"nrMQ/kitex_gen/api/zkserver_operations"
	"nrMQ/kitex_gen/raftoperations/raft_operations"
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
	zkclient zkserver_operation.Client
	mu       sync.RWMutex

	aplych    chan info
	topics    map[string]*Topic
	consumers map[string]*Client
	brokers   map[string]*raft_operations.Client

	//raft
	prafts_rafts *parts_raft

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
	Size       int64  `json:"size"`
	Topic_name string `json:"topic_name"`
	Part_name  string `json:"part_name"`
	Msg        []byte `json:"msg"`
}

type info struct {
	topic_name string
	part_name  string
	file_name  string
	new_name   string
	option     int8
	offset     int64
	size       int8

	ack int8

	producer string
	consumer string
	cmdindex int64
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

}

// 准备接收信息
// 检查topic和partition是否存在，不存在则需要创建
// 设置partition中的file和fd,start_index等信息
func (s *Server) PrepareAcceptHandle(in info) (ret string, err error) {

}
