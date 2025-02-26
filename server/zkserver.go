package server

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/cloudwego/kitex/client"
	"hash/crc32"
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
	Info_Topics     map[string]zookeeper.TopicNode
	Info_Partitions map[string]zookeeper.PartitionNode

	Brokers map[string]server_operations.Client //连接各个Broker

	//每个partition所在的集群 topic+partition to brokers
	PartToBro map[string][]string

	consistent *ConsistentBro
}

type Info_in struct {
	cliName   string
	topicName string
	partName  string
	blockName string
	index     int64
	option    int8
	dupNum    int8
}

type Info_out struct {
	Err         error
	brokerName  string
	broHostPort string
	Ret         string
}

func NewZKServer(zkinfo zookeeper.ZkInfo) *ZKServer {
	return &ZKServer{
		mu: sync.RWMutex{},
		zk: *zookeeper.NewZK(zkinfo),
	}
}

func (z *ZKServer) make(opt Options) {
	z.Name = opt.Name
	z.Info_Topics = make(map[string]zookeeper.TopicNode)
	z.Info_Partitions = make(map[string]zookeeper.PartitionNode)
	z.Brokers = make(map[string]server_operations.Client)
	z.PartToBro = make(map[string][]string)
}

func (z *ZKServer) CreateTopic(info Info_in) Info_out {
	tnode := zookeeper.TopicNode{Name: info.topicName}
	err := z.zk.RegisterNode(tnode)
	return Info_out{Err: err}
}

func (z *ZKServer) CreatePart(info Info_in) Info_out {
	pnode := zookeeper.PartitionNode{
		Name:      info.partName,
		TopicName: info.topicName,
	}

	err := z.zk.RegisterNode(pnode)
	if err != nil {
		return Info_out{Err: err}
	}

	return Info_out{Err: err}
}

// producer get broker
func (z *ZKServer) ProGetBroker(info Info_in) Info_out {
	//查询zookeeper,获得broker的host_port和name，若未连接则建立连接
	broker, block, err := z.zk.GetPartNowBrokerNode(info.topicName, info.partName)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}
	PartitionNode, err := z.zk.GetPartState(info.topicName, info.partName)
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
		Err:         err,
		brokerName:  broker.Name,
		broHostPort: broker.BrokHostPort,
		Ret:         ret,
	}
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

func (z *ZKServer) SubHandle(info Info_in) error {
	//在zookeeper上创建sub节点，若节点已经存在，则加入group中
	return nil
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
