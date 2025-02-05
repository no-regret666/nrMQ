package zookeeper

import (
	"encoding/json"
	"github.com/go-zookeeper/zk"
	"nrMQ/logger"
	"reflect"
	"time"
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

func NewZK(info *ZkInfo) *ZK {
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
	Name         string `json:"name"`
	BrokHostPort string `json:"brokerHostPort"`
	RaftHostPort string `json:"raftHostPort"`
	Me           int    `json:"me"`
	Pnum         int    `json:"pnum"`
}

type TopicNode struct {
	Name string `json:"name"`
	Pnum int    `json:"pnum"`
}

type PartitionNode struct {
	Name      string `json:"name"`
	TopicName string `json:"topicName"`
	Index     int64  `json:"index"`
	Option    int8   `json:"option"` //partition的状态
	DupNum    int8   `json:"dupNum"`
	PTPoffset int8   `json:"ptPoOffset"`
}

type SubscriptionNode struct {
	Name          string `json:"name"`
	TopicName     string `json:"topicName"`
	PartitionName string `json:"partitionName"`
	Option        int8   `json:"option"`
	Groups        []byte `json:"groups"`
}

type BlockNode struct {
	Name          string `json:"name"`
	FileName      string `json:"fileName"`
	TopicName     string `json:"topicName"`
	PartitionName string `json:"partitionName"`
	StartOffset   int64  `json:"startOffset"`
	EndOffset     int64  `json:"endOffset"`

	LeaderBroker string `json:"leaderBroker"`
}

type DuplicateNode struct {
	Name          string `json:"name"`
	TopicName     string `json:"topicName"`
	PartitionName string `json:"partitionName"`
	BlockName     string `json:"blockName"`
	StartOffset   int64  `json:"startOffset"`
	EndOffset     int64  `json:"endOffset"`
	BrokerName    string `json:"brokerName"`
}

type Map struct {
	Consumers map[string]bool `json:"consumers"`
}

func (z *ZK) RegisterNode(znode interface{}) (err error) {
	path := ""
	var data []byte
	var bnode BrokerNode
	var tnode TopicNode
	var pnode PartitionNode
	var blnode BlockNode
	var dnode DuplicateNode
	var snode SubscriptionNode

	i := reflect.TypeOf(znode)
	switch i.Name() {
	case "BrokerNode":
		bnode = znode.(BrokerNode) //断言
		path += z.BrokerRoot + "/" + bnode.Name
		data, err = json.Marshal(bnode)
	case "TopicNode":
		tnode = znode.(TopicNode)
		path += z.TopicRoot + "/" + tnode.Name
		data, err = json.Marshal(tnode)
	case "PartitionNode":
		pnode = znode.(PartitionNode)
		path += z.TopicRoot + "/" + pnode.TopicName + "/partitions/" + pnode.Name
		data, err = json.Marshal(pnode)
	case "SubscriptionNode":
		snode = znode.(SubscriptionNode)
		path += z.TopicRoot + "/" + snode.TopicName + "/subscriptions/" + snode.Name
		data, err = json.Marshal(snode)
	case "BlockNode":
		blnode = znode.(BlockNode)
		path += z.TopicRoot + "/" + blnode.TopicName + "/Partitions/" + blnode.PartitionName + "/" + blnode.Name
		data, err = json.Marshal(blnode)
	case "DuplicateNode":
		dnode = znode.(DuplicateNode)
		path += z.TopicRoot + "/" + dnode.TopicName + "/Partitions/" + dnode.PartitionName + "/" + dnode.BlockName + "/" + dnode.BrokerName
		data, err = json.Marshal(dnode)
	}
	if err != nil {
		logger.DEBUG(logger.DError, "the node %v turn json fail%v\n", path, err.Error())
	}

	ok, _, _ := z.conn.Exists(path)
	if ok {
		logger.DEBUG(logger.DLog, "the node %v had in zookeeper\n", path)
		_, sate, _ := z.conn.Get(path)
		z.conn.Set(path, data, sate.Version)
	} else {
		_, err = z.conn.Create(path, data, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			logger.DEBUG(logger.DError, "the node %v create fail%v\n", path, err.Error())
			return err
		}
	}

	if i.Name() == "TopicNode" {
		//创建Partitions和Subscriptions
		partitions_path := path + "/" + "Partitions"
		ok, _, err = z.conn.Exists(partitions_path)
		if ok {
			logger.DEBUG(logger.DLog, "the node %v had in zookeeper\n", partitions_path)
			return err
		}
		_, err = z.conn.Create(partitions_path, nil, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			logger.DEBUG(logger.DError, "the node %v create fail\n", partitions_path)
			return err
		}

		subscriptions_path := path + "/" + "Subscriptions"
		ok, _, err = z.conn.Exists(subscriptions_path)
		if ok {
			logger.DEBUG(logger.DLog, "the node %v had in zookeeper\n", subscriptions_path)
			return err
		}
		_, err = z.conn.Create(subscriptions_path, nil, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			logger.DEBUG(logger.DError, "the node %v create fail\n", subscriptions_path)
			return err
		}
	}

	return nil
}
