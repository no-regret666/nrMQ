package server

import (
	"context"
	"encoding/json"
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
	broker, block, _, err := z.zk.GetPartNowBrokerNode(info.topic_name, info.part_name)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}
	PartitionNode, err := z.zk.GetPartState(info.topic_name, info.part_name)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}

	//检查该Partition的状态是否设定
	//检查该Partition在Brokers上是否创建raft集群或fetch
	Brokers := make(map[string]string)
	var ret string
	Dups := z.zk.GetDuplicateNodes(block.TopicName, block.PartitionName, block.Name)
	for _, DupNode := range Dups {
		BrokerNode, err := z.zk.GetBrokerNode(DupNode.BrokerName)
		if err != nil {
			logger.DEBUG(logger.DError, "%v\n", err.Error())
		}
		Brokers[DupNode.BrokerName] = BrokerNode.BrokHostPort
	}

	data, err := json.Marshal(Brokers)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}

	for BrokerName, BrokerHostPort := range Brokers {
		z.mu.RLock()
		bro_cli, ok := z.Brokers[BrokerName]
		z.mu.RUnlock()

		//若未连接该broker
		if !ok {
			bro_cli, err := server_operations.NewClient(z.Name, client.WithHostPorts(BrokerHostPort))
			if err != nil {
				logger.DEBUG(logger.DError, "%v\n", err.Error())
			}
			z.mu.Lock()
			z.Brokers[BrokerName] = bro_cli
			z.mu.Unlock()
		}

		//通知broker检查topic/partition，并创建队列准备接收消息
		resp, err := bro_cli.PrepareAccept(context.Background())
	}
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

// 设置Partition的接收信息方式
// 若ack = -1,则为raft同步信息
// 若ack = 1,则leader写入, fetch获取信息
// 若ack = 0,则立即返回   , fetch获取信息
func (z *ZKServer) SetPartitionState(info Info_in) Info_out {
	var ret string
	var data_brokers []byte
	var Dups []zookeeper.DuplicateNode
	node, err := z.zk.GetPartState(info.topic_name, info.part_name)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return Info_out{
			Err: err,
		}
	}

	if info.option != node.Option {
		index, err := z.zk.GetPartBlockIndex(info.topic_name, info.part_name)
		if err != nil {
			logger.DEBUG(logger.DError, "%v\n", err.Error())
			return Info_out{
				Err: err,
			}
		}
		z.zk.UpdatePartitionNode(zookeeper.PartitionNode{
			TopicName: info.topic_name,
			Name:      info.part_name,
			Index:     index,
			Option:    info.option,
			PTPoffset: node.PTPoffset,
			DupNum:    info.dupnum, //需要下面的程序确认，是否能分配一定数量的副本
		})
	}

	logger.DEBUG(logger.DLog, "this partition(%v) status is %v\n", node.Name, node.Option)

	if node.Option == -2 {
		//未创建任何状态，即该partition未接收过任何信息

		switch info.option {
		case -1:
			//负载均衡获得一定数量broker节点，并在这些broker上部署raft集群
			//raft副本个数暂定默认3个

		}
	}
}
