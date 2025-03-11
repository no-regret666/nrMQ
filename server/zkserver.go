package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	repNum    int8
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

// 设置Partition的接收信息方式
// 若ack = -1,则为raft同步信息
// 若ack = 1,则leader写入,fetch获取信息
// 若ack = 0,则立刻返回，fetch获取信息
func (z *ZKServer) SetPartitionState(info Info_in) Info_out {
	var ret string
	var data_brokers []byte
	var reps []zookeeper.ReplicaNode
	node, err := z.zk.GetPartState(info.topicName, info.partName)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return Info_out{Err: err}
	}

	if info.option != node.Option {
		index, err := z.zk.GetPartBlockIndex(info.topicName, info.partName)
		if err != nil {
			logger.DEBUG(logger.DError, "%v\n", err)
			return Info_out{Err: err}
		}
		z.zk.UpdatePartitionNode(zookeeper.PartitionNode{
			TopicName: info.topicName,
			Name:      info.partName,
			Index:     index,
			Option:    info.option,
			PTPoffset: node.PTPoffset,
			RepNum:    info.repNum, //需要下面的程序确认，是否能分配一定数量的副本
		})
	}

	logger.DEBUG(logger.DLog, "this partition(%v) status is %v\n", node.Name, node.Option)

	if node.Option == -2 {
		//未创建任何状态，即该partition未接收过任何消息

		switch info.option {
		case -1:
			//raft副本个数暂时默认3个
			reps, data_brokers = z.GetRepsFromConsist(info)

			//向这些broker发送信息，启动raft
			for _, repNode := range reps {
				bro_cli, ok := z.Brokers[repNode.BrokerName]
				if !ok {
					logger.DEBUG(logger.DLog, "this partition(%v) leader broker is not connected\n", info.partName)
				} else {
					resp, err := bro_cli.AddRaftPartition(context.Background(), &api.AddRaftPartitionRequest{
						TopicName: info.topicName,
						PartName:  info.partName,
						Brokers:   data_brokers,
					})

					logger.DEBUG(logger.DLog, "the broker %v had add raft\n", repNode.BrokerName)
					if err != nil {
						logger.DEBUG(logger.DError, "%v err(%v)\n", resp, err.Error())
						return Info_out{Err: err}
					}
				}
			}
		default:
			//负载均衡获得一定数量broker节点，选择一个leader，并让其他节点fetch leader消息
			//默认副本数为3

			reps, data_brokers = z.GetRepsFromConsist(info)

			//选择一个broker节点作为leader
			LeaderBroker, err := z.zk.GetBrokerNode(reps[0].BrokerName)
			if err != nil {
				logger.DEBUG(logger.DError, "%v\n", err.Error())
				return Info_out{Err: err}
			}

			//更新newblock中的leader
			err = z.BecomeLeader(Info_in{
				cliName:   reps[0].BrokerName,
				topicName: info.topicName,
				partName:  info.partName,
			})

			if err != nil {
				logger.DEBUG(logger.DError, "%v\n", err.Error())
				return Info_out{Err: err}
			}

			for _, repNode := range reps {
				bro_cli, ok := z.Brokers[repNode.BrokerName]
				if !ok {
					logger.DEBUG(logger.DError, "this partition(%v) leader broker is not connected\n", info.partName)
				} else {
					//开启fetch机制
					resp3, err := bro_cli.AddFetchPartition(context.Background(), &api.AddFetchPartitionRequest{
						TopicName:    info.topicName,
						PartName:     info.partName,
						HostPort:     LeaderBroker.BrokHostPort,
						LeaderBroker: LeaderBroker.Name,
						FileName:     "NowBlock.txt",
						Brokers:      data_brokers,
					})

					if err != nil {
						logger.DEBUG(logger.DError, "%v err(%v)\n", resp3, err.Error())
						return Info_out{Err: err}
					}
				}
			}
		}

		return Info_out{
			Ret: ret,
		}
	}

	//获取该partition
}

// producer get broker
func (z *ZKServer) ProGetLeader(info Info_in) Info_out {
	//查询zookeeper,获得leaderBroker的host_port，若未连接则建立连接
	broker, block, err := z.zk.GetPartNowBrokerNode(info.topicName, info.partName)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return Info_out{Err: err}
	}
	PartitionNode, err := z.zk.GetPartState(info.topicName, info.partName)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return Info_out{Err: err}
	}

	//检查该Partition的状态是否设定
	//检查该Partition在Brokers上是否创建raft集群或fetch
	Brokers := make(map[string]string)
	var ret string
	replicas := z.zk.GetReplicaNodes(block.TopicName, block.PartName, block.Name)
	for _, replica := range replicas {
		BrokerNode, err := z.zk.GetBrokerNode(replica.BrokerName)
		if err != nil {
			logger.DEBUG(logger.DError, "%v\n", err.Error())
		}
		Brokers[replica.BrokerName] = BrokerNode.BrokHostPort
	}

	data, err := json.Marshal(Brokers)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}

	for BrokerName, BrokerHostPort := range Brokers {
		z.mu.RLock()
		bro_cli, ok := z.Brokers[BrokerName]
		z.mu.RUnlock()

		//未连接该broker
		if !ok {
			bro_cli, err = server_operations.NewClient(z.Name, client.WithHostPorts(BrokerHostPort))
			if err != nil {
				logger.DEBUG(logger.DError, "%v\n", err.Error())
			}
			z.mu.Lock()
			z.Brokers[BrokerName] = bro_cli
			z.mu.Unlock()
		}

		// 通知broker检查topic/partition，并创建队列准备接收信息
		resp, err := bro_cli.PrepareAccept(context.Background(), &api.PrepareAcceptRequest{
			TopicName: block.TopicName,
			PartName:  block.PartName,
			FileName:  block.FileName,
		})
		if err != nil || !resp.Ret {
			logger.DEBUG(logger.DError, "%v\n", err.Error())
		}

		//检查该Partition的状态是否恒定
		//检查该Partition在brokers上是否创建raft集群或fetch
		//若该Partition没有设置状态则返回通知producer
		if PartitionNode.Option == -2 { //未设置状态
			ret = "Partition State is -2"
		} else {
			resp, err := bro_cli.PrepareState(context.Background(), &api.PrepareStateRequest{
				TopicName: block.TopicName,
				PartName:  block.PartName,
				State:     PartitionNode.Option,
				Brokers:   data,
			})
			if err != nil || !resp.Ret {
				logger.DEBUG(logger.DError, "%v\n", err.Error())
			}
		}
	}

	//返回producer broker和host_port
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
	path := fmt.Sprintf(zookeeper.PNodePath, z.zk.TopicRoot, info.topicName, info.partName)
	_, err := z.zk.GetPartitionNode(path)
	if err != nil {
		logger.DEBUG(logger.DError, "the topic-partition(%v-%v) does not exist\n", info.topicName, info.partName)
		return errors.New("the topic-partition does not exist")
	}
	err = z.zk.RegisterNode(zookeeper.SubscriptionNode{
		Name:      info.cliName,
		TopicName: info.topicName,
		PartName:  info.partName,
		Subtype:   info.option,
	})
	if err != nil {
		return err
	}
	return nil
}

// consumer查询该向哪些broker发送请求
// zkserver让broker准备好topic/sub和config
func (z *ZKServer) HandStartGetBroker(info Info_in) (rets []byte, size int, err error) {
	var Parts []zookeeper.Part

	//检查该用户是否订阅了该topic/partition
	z.zk.CheckSub(zookeeper.StartGetInfo{
		CliName:       info.cliName,
		TopicName:     info.topicName,
		PartitionName: info.partName,
		Option:        info.option,
	})

	//获取该topic或partition的broker，并保证在线，若全部离线则Err
	if info.option == TOPIC_NIL_PTP_PULL || info.option == TOPIC_NIL_PTP_PUSH { //ptp_push
		Parts, err = z.zk.GetBrokers(info.topicName)
	} else if info.option == TOPIC_KEY_PSB_PULL || info.option == TOPIC_KEY_PSB_PUSH { //psb_push
		Parts, err = z.zk.GetBroker(info.topicName, info.partName, info.index)
	}
	if err != nil {
		return nil, 0, err
	}
	logger.DEBUG(logger.DLog, "the brokers are %v", Parts)

	//获取到信息后将通知brokers，让他们检查是否有该topic/partition/subscription/config等
	//并开启part发送协程，若协程在超时时间到后未收到管道的信息，则关闭协程
	partkeys := z.SendPreoare(Parts, info)

	parts := clients.Parts{
		PartKeys: partkeys,
	}
	data, err := json.Marshal(parts)

	var nodes clients.Parts

	json.Unmarshal(data, &nodes)

	logger.DEBUG(logger.DLog, "the partkeys %v and nodes %v\n", partkeys, nodes)
	if err != nil {
		logger.DEBUG(logger.DError, "turn partkeys to json failed\n", err.Error())
	}

	return data, size, nil
}

func (z *ZKServer) SendPreoare(Parts []zookeeper.Part, info Info_in) (partkeys []clients.PartKey) {
	for _, part := range Parts {
		if part.Err != OK {
			logger.DEBUG(logger.DLog, "the part.ERR(%v) != OK the part is %v\n", part.Err, part)
			partkeys = append(partkeys, clients.PartKey{
				Err: part.Err,
			})
			continue
		}
		z.mu.RLock()
		bro_cli, ok := z.Brokers[part.BrokerName]
		z.mu.RUnlock()

		if !ok {
			bro_cli, err := server_operations.NewClient(z.Name, client.WithHostPorts(part.BrokHost_Port))
			if err != nil {
				logger.DEBUG(logger.DError, "broker(%v) host_port(%v) can't connect %v", part.BrokerName, part.BrokHost_Port, err.Error())
			}
			z.mu.Lock()
			z.Brokers[part.BrokerName] = bro_cli
			z.mu.Unlock()
		}
		rep := &api.PrepareSendRequest{
			Consumer:  info.cliName,
			TopicName: info.topicName,
			PartName:  info.partName,
			FileName:  part.Filename,
			Option:    info.option,
		}
		if rep.Option == TOPIC_NIL_PTP_PULL || rep.Option == TOPIC_NIL_PTP_PUSH { //ptp
			rep.Offset = part.PTP_index
		} else if rep.Option == TOPIC_KEY_PSB_PULL || rep.Option == TOPIC_KEY_PSB_PUSH { //psb
			rep.Offset = info.index
		}
		resp, err := bro_cli.PrepareSend(context.Background(), rep)
		if err != nil || !resp.Ret {
			logger.DEBUG(logger.DError, "PrepareSend error %v", err.Error())
		}
		logger.DEBUG(logger.DLog, "the part is %v", part)
		partkeys = append(partkeys, clients.PartKey{
			Name:       part.PartName,
			BrokerName: part.BrokerName,
			Broker_H_P: part.BrokHost_Port,
			Offset:     part.PTP_index,
			Err:        part.Err,
		})
	}
	return partkeys
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
