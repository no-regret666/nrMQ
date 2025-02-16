package server

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/cloudwego/kitex/client"
	"hash/crc32"
	"nrMQ/client/clients"
	"nrMQ/kitex_gen/api"
	"nrMQ/kitex_gen/api/server_operations"
	"nrMQ/logger"
	"nrMQ/zookeeper"
	"sort"
	"strconv"
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

	consistent *ConsistentBro
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
		resp, err := bro_cli.PrepareAccept(context.Background(), &api.PrepareAcceptRequest{
			TopicName: block.TopicName,
			PartName:  block.PartitionName,
			FileName:  block.FileName,
		})
		if err != nil || !resp.Ret {
			logger.DEBUG(logger.DError, err.Error()+resp.Err)
		}

		//检查该Partition的状态是否设定
		//检查该Partition在Brokers上是否创建raft集群或fetch
		//若该Partition没有设置状态则返回通知producer
		if PartitionNode.Option == -2 { //未设置状态
			ret = "Partition State is -2"
		} else {
			resp, err := bro_cli.PrepareState(context.Background(), &api.PrepareStateRequest{
				TopicName: block.TopicName,
				PartName:  block.PartitionName,
				State:     PartitionNode.Option,
				Brokers:   data,
			})
			if err != nil || !resp.Ret {
				logger.DEBUG(logger.DError, "%v\n", err.Error())
			}
		}
	}

	//返回producer broker的host_port
	return Info_out{
		Err:           err,
		broker_name:   broker.Name,
		bro_host_port: broker.BrokHostPort,
		Ret:           ret,
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

func (z *ZKServer) CreateNowBlock(info Info_in) error {
	block_node := zookeeper.BlockNode{
		Name:          "NowBlock",
		FileName:      info.topic_name + info.part_name + "now.txt",
		TopicName:     info.topic_name,
		PartitionName: info.part_name,
		StartOffset:   int64(0),
	}
	return z.zk.RegisterNode(block_node)
}

func (z *ZKServer) SubHandle(info Info_in) error {
	//在zookeeper上创建sub节点，若节点已经存在，则加入group中
	return nil
}

// consumer查询应该向哪些broker发送请求
// zkserver让broker准备好topic/sub和config
func (z *ZKServer) HandleStartGetBroker(info Info_in) (rets []byte, size int, err error) {
	var Parts []zookeeper.Part

	//检查该用户是否订阅了该topic/partition
	z.zk.CheckSub(zookeeper.StartGetInfo{
		CliName:       info.cli_name,
		TopicName:     info.topic_name,
		PartitionName: info.part_name,
		Option:        info.option,
	})

	//获取该topic或partition的broker，并保证在线，若全部离线则Err
	if info.option == TOPIC_NIL_PTP_PULL || info.option == TOPIC_NIL_PTP_PUSH {
		Parts, err = z.zk.GetBrokers(info.topic_name)
	} else if info.option == TOPIC_KEY_PSB_PULL || info.option == TOPIC_KEY_PSB_PUSH {
		Parts, err = z.zk.GetBroker(info.topic_name, info.part_name, info.index)
	}
	if err != nil {
		return nil, 0, err
	}

	logger.DEBUG(logger.DLog, "the brokers is %v\n", Parts)

	//获取到该信息后将通知brokers，让他们检查是否有该Topic/Partition/Subscription/config等
	//并开启Part发送协程，若协程在超时时间到后未收到管道的信息，则关闭该协程
	partkeys := z.SendPreoare(Parts, info)

	parts := clients.Parts{
		PartKeys: partkeys,
	}
	data, err := json.Marshal(parts)

	var nodes clients.Parts

	json.Unmarshal(data, &nodes)

	logger.DEBUG(logger.DLog, "the partkeys %v and nodes is %v\n", partkeys, nodes)

	if err != nil {
		logger.DEBUG(logger.DError, "turn partkeys to json fail %v", err.Error())
	}

	return data, size, nil
}

type ConsistentBro struct {
	//排序的hash虚拟节点(环形)
	hashSortedNodes []uint32
	//虚拟节点(broker)对应的实际节点
	circle map[uint32]string
	//已绑定的broker为true
	nodes map[string]bool

	BroH map[string]bool

	mu sync.RWMutex
	//虚拟节点个数
	virtualNodeCount int
}

func NewConsistentBro() *ConsistentBro {
	con := &ConsistentBro{
		hashSortedNodes: make([]uint32, 2),
		circle:          make(map[uint32]string),
		nodes:           make(map[string]bool),
		BroH:            make(map[string]bool),

		mu:               sync.RWMutex{},
		virtualNodeCount: VIRTUAL_10,
	}

	return con
}

func (c *ConsistentBro) hashKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

// add consumer name as node
func (c *ConsistentBro) Add(node string, power int) error {
	if node == "" {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.nodes[node]; ok {
		return errors.New("node already existed")
	}
	c.nodes[node] = true

	for i := 0; i < c.virtualNodeCount*power; i++ {
		virtualKey := c.hashKey(node + strconv.Itoa(i))
		c.circle[virtualKey] = node
		c.hashSortedNodes = append(c.hashSortedNodes, virtualKey)
	}

	sort.Slice(c.hashSortedNodes, func(i, j int) bool {
		return c.hashSortedNodes[i] < c.hashSortedNodes[j]
	})

	return nil
}
