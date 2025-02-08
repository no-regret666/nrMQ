package server

import (
	"nrMQ/kitex_gen/api/server_operations"
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

}
