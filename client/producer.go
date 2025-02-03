package client

import (
	"context"
	"github.com/cloudwego/kitex/client"
	"nrMQ/kitex_gen/operations"
	"nrMQ/kitex_gen/operations/zkserver_operation"
	"sync"
)

type ProducerRecord struct {
	topic     string //主题
	partition string //分区
	key       string //键
	value     string //值
}

type Producer struct {
	mu sync.RWMutex

	name     string
	zkBroker zkserver_operation.Client
}

func newProducer(zkBroker string, name string) (*Producer, error) {
	p := Producer{
		mu:   sync.RWMutex{},
		name: name,
	}
	var err error
	p.zkBroker, err = zkserver_operation.NewClient(p.name, client.WithHostPorts(zkBroker))

	return &p, err
}

func (p *Producer) createTopic(topicName string) error {
	resp, err := p.zkBroker.CreateTopic(context.Background(), &operations.CreateTopicRequest{
		TopicName: topicName,
	})
	if err != nil || !resp.Ok {
		return err
	}
	return nil
}

func (p *Producer) createPart(topicName, partName string) error {
	resp, err := p.zkBroker.CreatePart(context.Background(), &operations.CreatePartRequest{
		TopicName: topicName,
		PartName:  partName,
	})

	if err != nil || !resp.Ok {
		return err
	}
	return nil
}

func (p *Producer) send(msg *ProducerRecord) {

}
