package client

import (
	"context"
	"errors"
	"github.com/cloudwego/kitex/client"
	"nrMQ/kitex_gen/api"
	"nrMQ/kitex_gen/api/server_operations"
	"nrMQ/kitex_gen/api/zkserver_operations"
	"sync"
)

type ProducerRecord struct {
	Topic     string //主题
	Partition string //分区
	Msg       []byte //信息
}

type Producer struct {
	mu sync.RWMutex

	Name            string //唯一标识
	ZkBroker        zkserver_operations.Client
	TopicPartitions map[string]server_operations.Client //map[topicname+partname]cli：缓存主题分区和broker关系
	TopPartIndexs   map[string]int64
}

func NewProducer(zkBroker string, name string) (*Producer, error) {
	p := Producer{
		mu:              sync.RWMutex{},
		Name:            name,
		TopicPartitions: make(map[string]server_operations.Client),
		TopPartIndexs:   make(map[string]int64),
	}
	var err error
	p.ZkBroker, err = zkserver_operations.NewClient(p.Name, client.WithHostPorts(zkBroker))

	return &p, err
}

func (p *Producer) CreateTopic(topicName string) error {
	resp, err := p.ZkBroker.CreateTopic(context.Background(), &api.CreateTopicRequest{
		TopicName: topicName,
	})
	if err != nil || !resp.Ret {
		return err
	}
	return nil
}

func (p *Producer) CreatePart(topicName, partName string) error {
	resp, err := p.ZkBroker.CreatePart(context.Background(), &api.CreatePartRequest{
		TopicName: topicName,
		PartName:  partName,
	})

	if err != nil || !resp.Ret {
		return err
	}
	return nil
}

func (p *Producer) SetPartitionState(topicName, partName string, option, dupnum int8) error {
	resp, err := p.ZkBroker.SetPartitionState(context.Background(), &api.SetPartitionStateRequest{
		Topic:     topicName,
		Partition: partName,
		Option:    option,
		Dupnum:    dupnum,
	})

	if err != nil || !resp.Ret {
		return err
	}
	return nil
}

func (p *Producer) Push(msg ProducerRecord, ack int) error {
	str := msg.Topic + msg.Partition
	var ok2 bool
	var index int64
	p.mu.RLock()
	cli, ok1 := p.TopicPartitions[str]
	if ack == -1 { //不理解wwwwwwwww
		index, ok2 = p.TopPartIndexs[str]
	}
	zk := p.ZkBroker
	p.mu.RUnlock()

	if !ok1 {
		resp, err := zk.ProGetBroker(context.Background(), &api.ProGetBrokRequest{
			TopicName: msg.Topic,
			PartName:  msg.Partition,
		})
		if err != nil || !resp.Ret {
			return err
		}

		cli, err = server_operations.NewClient(p.Name, client.WithHostPorts(resp.BrokerHostPort))

		if err != nil {
			return err
		}

		p.mu.Lock()
		p.TopicPartitions[str] = cli
		p.mu.Unlock()
	}

	if ack == -1 {
		if !ok2 {
			p.mu.Lock()
			p.TopPartIndexs[str] = 0
			p.mu.Unlock()
		}
	}

	//若partition所在的broker发生改变，将重新发送该信息，重新请求zkserver
	resp, err := cli.Push(context.Background(), &api.PushRequest{
		Producer: p.Name,
		Topic:    msg.Topic,
		Key:      msg.Partition,
		Message:  msg.Msg,
		Ack:      int8(ack),
		CmdIndex: index,
	})
	if err == nil || resp.Ret {
		p.mu.Lock()
		p.TopPartIndexs[str] = index + 1
		p.mu.Unlock()

		return nil
	} else if resp.Err == "partition remove" {
		p.mu.Lock()
		delete(p.TopicPartitions, str)
		p.mu.Unlock()

		return p.Push(msg, ack)
	} else {
		return errors.New("err == " + err.Error() + "or resp.Ret == false")
	}
}
