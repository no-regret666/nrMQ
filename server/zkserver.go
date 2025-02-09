package server

import (
	"github.com/cloudwego/kitex/client"
	"nrMQ/kitex_gen/api/server_operations"
	"nrMQ/logger"
	"nrMQ/zookeeper"
	"sync"
)

type ZKServer struct {
	mu              sync.RWMutex
	zk              zookeeper.ZK
	Name            string
	Info_Brokers    map[string]zookeeper.BlockNode
	Info_Topics     map[string]zookeeper.TopicNode
	Info_Partitions map[string]zookeeper.PartitionNode

	Brokers map[string]server_operations.Client //连接各个Broker

	//每个partition所在的集群 topic+partition to brokers
	PartToBro map[string][]string
}

type Info_in struct {
	cli_name   string
	topic_name string
	part_name  string
	blockname  string
	index      int64
	option     int8
	dupnum     int8
}

type Info_out struct {
	Err           error
	broker_name   string
	bro_host_port string
	Ret           string
}

func NewZKServer(zkinfo zookeeper.ZkInfo) *ZKServer {
	return &ZKServer{
		mu: sync.RWMutex{},
		zk: *zookeeper.NewZK(zkinfo),
	}
}

func (z *ZKServer) make(opt Options) {
	z.Name = opt.Name
	z.Info_Brokers = make(map[string]zookeeper.BlockNode)
	z.Info_Topics = make(map[string]zookeeper.TopicNode)
	z.Info_Partitions = make(map[string]zookeeper.PartitionNode)
	z.Brokers = make(map[string]server_operations.Client)
	z.PartToBro = make(map[string][]string)
}

func (z *ZKServer) HandleBroInfo(bro_name, bro_H_P string) error {
	bro_cli, err := server_operations.NewClient(z.Name, client.WithHostPorts(bro_H_P))
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return err
	}
	z.mu.Lock()
	z.Brokers[bro_name] = bro_cli
	z.mu.Unlock()

	return nil
}

func (z *ZKServer) ProGetBroker(info Info_in) Info_out {
	//查询zookeeper,获得broker的host_port和name，若未连接则建立连接

}

func (z *ZKServer) CreateTopic(info Info_in) Info_out {
	tnode := zookeeper.TopicNode{Name: info.topic_name}
	err := z.zk.RegisterNode(tnode)
	return Info_out{Err: err}
}

func (z *ZKServer) CreatePart(info Info_in) Info_out {
	pnode := zookeeper.PartitionNode{
		Name:      info.part_name,
		Index:     int64(1),
		TopicName: info.topic_name,
		Option:    -2,
		PTPoffset: int64(0),
	}

	err := z.zk.RegisterNode(pnode)
	if err != nil {
		return Info_out{Err: err}
	}

	err = z.CreateNowBlock(info)
	return Info_out{Err: err}
}
