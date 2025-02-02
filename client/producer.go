package client

import (
	"context"
	"nrMQ/kitex_gen/api"
	"nrMQ/kitex_gen/api/zkserver_operation"
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
	return &p, err
}

func (p *Producer) createTopic(newTopic string, queueNum int) error {
	resp, err := p.zkBroker.CreateTopic(context.Background(), &api.CreateTopicRequest{
		TopicName: newTopic,
	})
	if err != nil || !resp.Ok {
		return err
	}
	return nil
}

func (p *Producer) send(msg *ProducerRecord) {

}
