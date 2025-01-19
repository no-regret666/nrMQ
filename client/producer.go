package client

import "time"

type ProducerRecord struct {
	topic string //主题
	body  []byte //消息体
}

type Producer struct {
	ID           string
	Name         string
	Topic        string
	Retries      int
	BatchSize    int
	Timeout      time.Duration
	ErrorChannel chan error
}

func newProducer() *Producer {

}

func (p *Producer) createTopic(newTopic string, queueNum int) error {

}

func (p *Producer) send(msg *ProducerRecord) {

}
