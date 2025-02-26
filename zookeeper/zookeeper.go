package zookeeper

import (
	"encoding/json"
	"fmt"
	"github.com/go-zookeeper/zk"
	"nrMQ/logger"
	"reflect"
	"time"
)

var (
	BNodePath  = "%v/%v"                     // BrokerRoot/BrokerName
	TNodePath  = "%v/%v/%v"                  // TopicRoot/TopicName
	PNodePath  = "%v/%v/partitions/%v"       // TopicRoot/TopicName/partitions/PartName
	BlNodePath = "%v/%v/partitions/%v/%v"    // PNodePath/BlockName
	DNodePath  = "%v/%v/partitions/%v/%v/%v" // BlNodePath/DuplicateName
)

type ZK struct {
	conn *zk.Conn

	Root       string
	BrokerRoot string
	TopicRoot  string
}

type ZkInfo struct {
	HostPorts []string //
	Timeout   int      //zookeeper连接的超时时间
	Root      string
}

func NewZK(info ZkInfo) *ZK {
	conn, _, err := zk.Connect(info.HostPorts, time.Duration(info.Timeout)*time.Second)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}
	return &ZK{
		conn:       conn,
		Root:       info.Root,
		BrokerRoot: info.Root + "/Brokers",
		TopicRoot:  info.Root + "/Topics",
	}
}

type BrokerNode struct {
	Name string `json:"name"`
	Host string `json:"host"`
}

type TopicNode struct {
	Name    string `json:"name"`
	PartNum int    `json:"part_num"`
}

type PartitionNode struct {
	Name      string `json:"name"`
	TopicName string `json:"topicName"`
}

type BlockNode struct {
	Name          string `json:"name"` //part所在broker
	TopicName     string `json:"topicName"`
	PartitionName string `json:"partitionName"`

	LeaderBroker string `json:"leaderBroker"`
}

type Map struct {
	Consumers map[string]bool `json:"consumers"`
}

func (z *ZK) RegisterNode(znode interface{}) (err error) {
	path := ""
	var data []byte
	var bnode BrokerNode

	i := reflect.TypeOf(znode)
	switch i.Name() {
	case "BrokerNode":
		bnode = znode.(BrokerNode)
		path = fmt.Sprintf(BNodePath, z.BrokerRoot, bnode.Name)
		data, err = json.Marshal(bnode)
	}

	if err != nil {
		logger.DEBUG(logger.DError, "the node %v turn json failed %v\n", path, err.Error())
		return err
	}
	ok, _, _ := z.conn.Exists(path)
	if ok {
		logger.DEBUG(logger.DLog, "the node %v had in zookeeper\n", path)
		_, sata, _ := z.conn.Get(path)
		z.conn.Set(path, data, sata.Version)
	} else {
		_, err = z.conn.Create(path, data, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			logger.DEBUG(logger.DError, "the node %v create failed %v\n", path, err.Error())
			return err
		}
	}

	return nil
}

func (z *ZK) UpdatePartitionNode(pnode PartitionNode) error {
	path := z.TopicRoot + "/" + pnode.TopicName + "/partitions/" + pnode.Name
	ok, _, err := z.conn.Exists(path)
	if !ok {
		return err
	}
	data, err := json.Marshal(pnode)
	if err != nil {
		return err
	}
	_, sate, _ := z.conn.Get(path)
	_, err = z.conn.Set(path, data, sate.Version)
	if err != nil {
		return err
	}

	return nil
}

func (z *ZK) GetPartState(topic_name, part_name string) (PartitionNode, error) {
	var node PartitionNode
	path := z.TopicRoot + "/" + topic_name + "/Partitions/" + part_name
	ok, _, err := z.conn.Exists(path)
	if !ok {
		return node, err
	}
	data, _, _ := z.conn.Get(path)

	err = json.Unmarshal(data, &node)

	return node, nil
}

func (z *ZK) CreateState(name string) error {
	path := z.TopicRoot + "/" + name + "/state"
	ok, _, err := z.conn.Exists(path)
	logger.DEBUG(logger.DLog, "create broker state %v ok %v\n", path, ok)
	if ok {
		return err
	}
	_, err = z.conn.Create(path, nil, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err != nil {
		return err
	}
	return nil
}

type Part struct {
	TopicName     string
	PartName      string
	BrokerName    string
	BrokHost_Port string
	RaftHost_Port string
	PTP_index     int64
	Filename      string
	Err           string
}

// 检查broker是否在线
func (z *ZK) CheckBroker(BrokerName string) bool {
	path := z.BrokerRoot + "/" + BrokerName + "/state"
	ok, _, _ := z.conn.Exists(path)
	logger.DEBUG(logger.DLog, "state(%v) path is %v\n", ok, path)
	return ok
}

type StartGetInfo struct {
	CliName       string
	TopicName     string
	PartitionName string
	Option        int8
}

func (z *ZK) CheckSub(info StartGetInfo) bool {

	//检查该consumer是否订阅了该topic或partition

	return true
}

func (z *ZK) GetBrokerNode(name string) (BrokerNode, error) {
	path := z.BrokerRoot + "/" + name
	var bronode BrokerNode
	ok, _, err := z.conn.Exists(path)
	if !ok {
		return bronode, err
	}
	data, _, _ := z.conn.Get(path)
	err = json.Unmarshal(data, &bronode)
	return bronode, nil
}

func (z *ZK) GetPartitionNode(path string) (PartitionNode, error) {
	var pnode PartitionNode
	ok, _, err := z.conn.Exists(path)
	if !ok {
		return pnode, err
	}
	data, _, _ := z.conn.Get(path)
	err = json.Unmarshal(data, &pnode)

	return pnode, nil
}

func (z *ZK) GetBlockNode(path string) (BlockNode, error) {
	var blnode BlockNode
	ok, _, err := z.conn.Exists(path)
	if !ok {
		return blnode, err
	}
	data, _, _ := z.conn.Get(path)
	err = json.Unmarshal(data, &blnode)
	return blnode, nil
}

// 若leader不在线，则等待一秒继续请求
func (z *ZK) GetPartNowBrokerNode(topicName, partName string) (BrokerNode, BlockNode, error) {
	now_block_path := fmt.Sprintf(BlNodePath, z.TopicRoot, topicName, partName, "NowBlock")
	for {
		NowBlock, err := z.GetBlockNode(now_block_path)
		if err != nil {
			logger.DEBUG(logger.DError, "get block node failed,path %v err is %v\n", now_block_path, err.Error())
			return BrokerNode{}, BlockNode{}, err
		}
		Broker, err := z.GetBrokerNode(NowBlock.LeaderBroker)
		if err != nil {
			logger.DEBUG(logger.DError, "get broker node failed,name %v err is %v\n", NowBlock.LeaderBroker, err.Error())
			return BrokerNode{}, BlockNode{}, err
		}
		logger.DEBUG(logger.DLog, "the leader broker is %v\n", NowBlock.LeaderBroker)
		ret := z.CheckBroker(Broker.Name)

		if ret {
			return Broker, NowBlock, nil
		} else {
			logger.DEBUG(logger.DLog, "the broker %v is not online\n", NowBlock.LeaderBroker)
			time.Sleep(time.Second * 1)
		}
	}
}
