package server

import (
	"errors"
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

func (t *Topic) PrepareAcceptHandle(in info) (ret string, err error) {
	t.mu.Lock()
	partition, ok := t.Parts[in.part_name]
	if !ok {
		partition = NewPartition(t.Broker, t.Name, in.part_name)
		t.Parts[in.part_name] = partition
	}

	//设置partition中的file和fd,start_index等信息
	str, _ := os.Getwd()
	str += "/" + t.Broker + "/" + in.topic_name + "/" + in.part_name + "/" + in.file_name
	file, fd, Err, err := newFile(str)
	if err != nil {
		return Err, err
	}
	t.Files[str] = file
	t.mu.Unlock()
	ret = partition.StartGetMessage(file, fd, in)
	if ret == OK {
		logger.DEBUG(logger.DLog, "topic(%v)_partition(%v) Start success\n", in.topic_name, in.part_name)
	} else {
		logger.DEBUG(logger.DLog, "topic(%v)_partition(%v) had started\n", in.topic_name, in.part_name)
	}
	return ret, nil
}

func (t *Topic) CloseAcceptPart(in info) (start, end int64, ret string, err error) {
	t.mu.Lock()
	partition, ok := t.Parts[in.part_name]
	t.mu.Unlock()
	if !ok {
		ret = "this partition is not in the broker"
		logger.DEBUG(logger.DError, "this partition (%v) is not in this broker\n", in.part_name)
		return 0, 0, ret, errors.New(ret)
	}
	start, end, ret, err = partition.CloseAcceptMessage(in)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	} else {
		str, _ := os.Getwd()
		str += "/" + t.Broker + "/" + in.topic_name + "/" + in.part_name + "/"
		t.mu.Lock()
		t.Files[str+in.new_name] = t.Files[str+in.file_name]
		delete(t.Files, str+in.file_name)
		t.mu.Unlock()
	}
	return start, end, ret, err
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

const (
	ErrHadStart = "this partition had start"
	OK          = "start partition successfully"
)

func (p *Partition) StartGetMessage(file *File, fd *os.File, in info) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	ret := ""
	switch p.state {
	case ALIVE:
		ret = ErrHadStart
	case CLOSE:
		p.state = ALIVE
		p.file = file
		p.fd = fd
		p.file_name = in.file_name
		p.index = file.GetIndex(fd)
		p.start_index = p.index
		ret = OK
	}
	return ret
}

func (p *Partition) CloseAcceptMessage(in info) (start, end int64, ret string, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.state == ALIVE {
		str, _ := os.Getwd()
		str += "/" + p.Broker + "/" + in.topic_name + "/" + in.part_name
		err = p.file.Update(str, in.new_name) //修改本地文件名
		p.file_name = in.new_name
		p.state = DOWN
		end = p.index
		start = p.file.GetFirstIndex(p.fd)
		p.fd.Close()
	} else if p.state == DOWN {
		ret = "this partition had close"
		logger.DEBUG(logger.DLog, "%v\n", ret)
		err = errors.New(ret)
	}
	return start, end, ret, err
}

func (p *Partition) GetFile() *File {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.file
}

type SubScription struct {
	rmu        sync.RWMutex
	name       string
	topic_name string

	option int8 //PTP / PSB

}
