package server

import (
	"nrMQ/logger"
	"os"
	"sync"
)

const (
	TOPIC_NIL_PTP_PUSH = int8(1) //PTP---Push
	TOPIC_NIL_PTP_PULL = int8(2)
	TOPIC_KEY_PSB_PUSH = int8(3) //map[cli_name]offset in a position
	TOPIC_KEY_PSB_PULL = int8(4)

	TOPIC_NIL_PSB = 10

	VIRTUAL_1  = 1
	VIRTUAL_10 = 10
	VIRTUAL_20 = 20
	VIRTUAL_30 = 30
	VIRTUAL_40 = 40
	VIRTUAL_50 = 50

	OFFSET = 0
)

type Topic struct {
	mu      sync.RWMutex
	Broker  string
	Name    string
	Files   map[string]*File
	Parts   map[string]*Partition
	subList map[string]*SubScription
}

func NewTopic(broker_name, topic_name string) *Topic {
	topic := &Topic{
		mu:      sync.RWMutex{},
		Broker:  broker_name,
		Name:    topic_name,
		Parts:   make(map[string]*Partition),
		subList: make(map[string]*SubScription),
		Files:   make(map[string]*File),
	}
	str, _ := os.Getwd()
	str += "/" + broker_name + "/" + topic_name
	CreateList(str) //则存在，则不会创建

	return topic
}

func (t *Topic) addMessage(in info) error {
	t.mu.RLock()
	part, ok := t.Parts[in.part_name]
	t.mu.RUnlock()

	if !ok {
		logger.DEBUG(logger.DError, "not find this part in add message\n")
		part := NewPartition(t.Broker, in.topic_name, in.part_name) //new a Partition //需要向sub中和config中加入一个partition
		t.mu.Lock()
		t.Parts[in.part_name] = part
		t.mu.Unlock()
	}

	logger.DEBUG(logger.DLog, "topic(%v) use partition(%v) addMessage\n", t.Name, in.part_name)
	part.AddMessage(in)

	return nil
}

const (
	START = "start"
	CLOSE = "close"
)

type Partition struct {
	mu     sync.RWMutex
	Broker string
	key    string
	state  string

	file_name   string
	file        *File
	fd          *os.File
	index       int64
	start_index int64
	queue       []Message
}

func NewPartition(broker_name, topic_name, part_name string) *Partition {
	part := &Partition{
		mu:     sync.RWMutex{},
		Broker: broker_name,
		state:  CLOSE,
		key:    part_name,
		index:  0,
	}

	str, _ := os.Getwd()
	str += "/" + broker_name + "/" + topic_name + "/" + part_name
	CreateList(str) //若存在，则不会创建

	return part
}

type SubScription struct {
	rmu        sync.RWMutex
	name       string
	topic_name string

	option int8 //PTP / PSB

}
