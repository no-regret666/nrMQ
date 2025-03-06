package server

import (
	"context"
	"encoding/json"
	"errors"
	"nrMQ/kitex_gen/api"
	"nrMQ/kitex_gen/api/client_operations"
	"nrMQ/kitex_gen/api/zkserver_operations"
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
	partition, ok := t.Parts[in.partName]
	if !ok {
		partition = NewPartition(t.Broker, t.Name, in.partName)
		t.Parts[in.partName] = partition
	}

	//设置partition中的file和fd,start_index等信息
	str, _ := os.Getwd()
	str += "/" + t.Broker + "/" + in.topicName + "/" + in.partName + "/" + in.fileName
	file, fd, Err, err := NewFile(str)
	if err != nil {
		return Err, err
	}
	t.Files[str] = file
	t.mu.Unlock()
	ret = partition.StartGetMessage(file, fd, in)
	if ret == OK {
		logger.DEBUG(logger.DLog, "topic(%v)_partition(%v) Start success\n", in.topicName, in.partName)
	} else {
		logger.DEBUG(logger.DLog, "topic(%v)_partition(%v) had started\n", in.topicName, in.partName)
	}
	return ret, nil
}

func (t *Topic) CloseAcceptPart(in info) (start, end int64, ret string, err error) {
	t.mu.Lock()
	partition, ok := t.Parts[in.partName]
	t.mu.Unlock()
	if !ok {
		ret = "this partition is not in the broker"
		logger.DEBUG(logger.DError, "this partition (%v) is not in this broker\n", in.partName)
		return 0, 0, ret, errors.New(ret)
	}
	start, end, ret, err = partition.CloseAcceptMessage(in)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	} else {
		str, _ := os.Getwd()
		str += "/" + t.Broker + "/" + in.topicName + "/" + in.partName + "/"
		t.mu.Lock()
		t.Files[str+in.newName] = t.Files[str+in.fileName]
		delete(t.Files, str+in.fileName)
		t.mu.Unlock()
	}
	return start, end, ret, err
}

func (t *Topic) addMessage(in info) error {
	t.mu.RLock()
	part, ok := t.Parts[in.partName]
	t.mu.RUnlock()

	if !ok {
		logger.DEBUG(logger.DError, "not find this part in add message\n")
		part := NewPartition(t.Broker, in.topicName, in.partName) //new a Partition //需要向sub中和config中加入一个partition
		t.mu.Lock()
		t.Parts[in.partName] = part
		t.mu.Unlock()
	}

	logger.DEBUG(logger.DLog, "topic(%v) use partition(%v) addMessage\n", t.Name, in.partName)
	part.AddMessage(in)

	return nil
}

func (t *Topic) PrepareSendHandle(in info, zkclient *zkserver_operations.Client) (ret string, err error) {
	sub_name := GetStringfromSub(in.topicName, in.partName, in.option)

	t.mu.Lock()
	//检查或创建partition
	partition, ok := t.Parts[in.partName]
	if !ok {
		partition = NewPartition(t.Broker, t.Name, in.partName)
		t.Parts[in.partName] = partition
	}

	//检查文件是否存在，若存在为获得File则创建File，若没有则返回错误
	str, _ := os.Getwd()
	str += "/" + t.Broker + "/" + in.topicName + "/" + in.partName + "/" + in.fileName
	file, ok := t.Files[str]
	if !ok {
		file_ptr, fd, Err, err := CheckFile(str)
		if err != nil {
			return Err, err
		}
		fd.Close()
		file = file_ptr
		t.Files[str] = file
	}

	//检查或创建sub
	sub, ok := t.subList[sub_name]
	if !ok {
		logger.DEBUG(logger.DLog, "%v create a new sub(%v)\n", t.Broker, sub_name)
		sub = NewSubScription(in, sub_name, t.Parts, t.Files)
		t.subList[sub_name] = sub
	}
	t.mu.Unlock()

	//在sub中创建对应文件的config，来等待startget
	if in.option == TOPIC_NIL_PTP_PUSH {
		ret, err = sub.AddPTPConfig(in, partition, file, zkclient)
	} else if in.option == TOPIC_KEY_PSB_PUSH {
		sub.AddPSBConfig(in, in.partName, file, zkclient)
	} else if in.option == TOPIC_NIL_PTP_PULL || in.option == TOPIC_KEY_PSB_PULL {
		//在sub中创建一个Node用来保存该consumer的Pull的文件描述符等信息
		logger.DEBUG(logger.DLog, "the file is %v\n", file)
		sub.AddNode(in, file)
	}

	return ret, err
}

// topic + "nil" + "ptp" (point to point consumer : partition为 1 : n)
// topic + key + "psb" (pub and sub consumer : partition 为 n : 1)
// topic + "nil" + "psb" (pub and sub consumer : partition 为 n : n)
func GetStringfromSub(topicName, partName string, option int8) string {
	ret := topicName
	if option == TOPIC_NIL_PTP_PUSH || option == TOPIC_NIL_PTP_PULL { //point to point
		ret = ret + "nil" + "ptp"
	} else if option == TOPIC_KEY_PSB_PUSH || option == TOPIC_KEY_PSB_PULL { //pub and sub
		ret = ret + partName + "psb"
	}
	return ret
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
		p.file_name = in.fileName
		p.index, _ = file.GetIndex(fd)
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
		str += "/" + p.Broker + "/" + in.topicName + "/" + in.partName
		err = p.file.Update(str, in.newName) //修改本地文件名
		p.file_name = in.newName
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

func (p *Partition) AddMessage(in info) (ret string, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.state == DOWN {
		ret := "this partition had close"
		logger.DEBUG(logger.DLog, "%v\n", ret)
		return ret, errors.New(ret)
	}
	p.index++
	msg := Message{
		Index:      p.index,
		Size:       in.size,
		Topic_name: in.topicName,
		Part_name:  in.partName,
		Msg:        in.message,
	}
	logger.DEBUG(logger.DLog, "part_name(%v) add message %v index is %v size is %v\n", p.key, msg, p.index, p.index-p.start_index)

	p.queue = append(p.queue, msg) //加入队列

	//达到一定大小后写入磁盘
	if p.index-p.start_index >= VIRTUAL_10 {
		var msg []Message
		for i := 0; i < VIRTUAL_10; i++ {
			msg = append(msg, p.queue[i])
		}

		node := Key{
			Start_index: p.start_index,
			End_index:   p.start_index + VIRTUAL_10 - 1,
		}

		data_msg, err := json.Marshal(node)
		if err != nil {
			logger.DEBUG(logger.DLog, "%v turn json fail\n", msg)
		}
		node.Size = int64(len(data_msg))

		logger.DEBUG(logger.DLog, "need write msgs size is (%v)", node.Size)
		if !p.file.WriteFile(p.fd, node, data_msg) {
			logger.DEBUG(logger.DError, "write to %v fail\n", p.file_name)
		} else {
			logger.DEBUG(logger.DLog, "%d write to %v success msgs %v\n", in.me, p.file_name, msg)
		}
		p.start_index += VIRTUAL_10 + 1
		p.queue = p.queue[VIRTUAL_10:]
	}

	(*in.zkclient).UpdateRep(context.Background(), &api.UpdateRepRequest{
		Topic:      in.topicName,
		Part:       in.partName,
		BrokerName: in.BrokerName,
		BlockName:  GetBlockName(in.fileName),
		EndIndex:   p.index,
	})

	return ret, err
}

type SubScription struct {
	mu         sync.RWMutex
	name       string
	topic_name string

	option int8 // PTP / PSB

	groups []*Group

	partitions map[string]*Partition
	Files      map[string]*File

	//需要修改，一个订阅需要多个config，因为一个partition有多个文件，一个文件需要一个config
	//需要修改，分为多种订阅，每种订阅方式一种config
	PTP_config *Config

	//partition_name + consumer_name to config
	PSB_configs map[string]*PSBConfig_PUSH

	//一个consumer向文件描述符等的映射，每次pull将使用上次的文件描述符等资源
	//topic+partition+consumer to Node
	nodes map[string]*Node
}

func NewSubScription(in info, name string, parts map[string]*Partition, files map[string]*File) *SubScription {
	sub := &SubScription{
		mu:          sync.RWMutex{},
		name:        name,
		topic_name:  in.topicName,
		partitions:  parts,
		Files:       files,
		PTP_config:  nil,
		PSB_configs: make(map[string]*PSBConfig_PUSH),
		nodes:       make(map[string]*Node),
	}

	group := NewGroup(in.partName, in.consumer)
	sub.groups = append(sub.groups, group)
	return sub
}

// 当有消费者需要开始消费时，PTP
// 若sub中该文件的config存在，则加入该config
// 若sub中该文件的config不存在，则创建一个config，并加入
func (s *SubScription) AddPTPConfig(in info, partition *Partition, file *File, zkclient *zkserver_operations.Client) (ret string, err error) {
	s.mu.RLock()
	if s.PTP_config == nil {
		s.PTP_config = NewConfig(in.topicName, 0, nil, nil)
	}

	err = s.PTP_config.AddPartition(in, partition, file, zkclient)
	s.mu.RUnlock()
	if err != nil {
		return err.Error(), err
	}
	return ret, nil
}

func (s *SubScription) AddPSBConfig(in info, partName string, file *File, zkclient *zkserver_operations.Client) (ret string, err error) {
	s.mu.RLock()
	_, ok := s.PSB_configs[partName+in.consumer]
	if !ok {
		config := NewPSBConfigPush(in, file, zkclient)
		s.PSB_configs[partName+in.consumer] = config
	} else {
		logger.DEBUG(logger.DLog, "This PSB has Start\n")
	}

	s.mu.RUnlock()
}

func (s *SubScription) ShutdownConsumerInGroup(cliName string) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch s.option {
	case TOPIC_NIL_PTP_PUSH: //point to point just one group
		s.groups[0].DownClient(cliName)
	case TOPIC_KEY_PSB_PUSH:
		for _, group := range s.groups {
			group.DownClient(cliName)
		}
	}

	return s.topic_name
}

type Config struct {
	mu       sync.RWMutex
	part_num int //partition数
	con_num  int //consumer数

	part_close chan *Part

	PartToCon map[string][]string

	Partitions map[string]*Partition
	Files      map[string]*File
	Clis       map[string]*client_operations.Client

	parts map[string]*Part //partitionName to Part

	consistent *Consistent
}

func NewConfig(topicName string, partNum int, partitions map[string]*Partition, files map[string]*File) *Config {
	config := &Config{
		mu:       sync.RWMutex{},
		part_num: partNum,
		con_num:  0,

		part_close: make(chan *Part),

		PartToCon:  make(map[string][]string),
		Files:      files,
		Partitions: partitions,
		Clis:       make(map[string]*client_operations.Client),
		parts:      make(map[string]*Part),
		consistent: NewConsistent(),
	}

	go config.GetCloseChan(config.part_close)

	return config
}

func (c *Config) GetCloseChan(ch chan *Part) {
	for close := range ch {
		c.DeletePartition(close.partName, close.file)
	}
}

// part消费完成，移除config中的Partition和Part
func (c *Config) DeletePartition(partName string, file *File) {
	c.mu.Lock()

	c.part_num--
	delete(c.parts, partName)
	delete(c.Files, partName)

	//该Part协程已经关闭，该partition的文件已经消费完毕
	c.mu.Unlock()

	c.RebalancePtoC() //更新配置
	c.UpdateParts()   //应用配置
}

func (c *Config) AddPartition(in info, partition *Partition, file *File, zkclient *zkserver_operations.Client) error {
	c.mu.Lock()

	c.part_num++
	c.Partitions[in.partName] = partition
	c.Files[in.fileName] = file

	c.parts[in.partName] = NewPart(in, file, zkclient)
	c.parts[in.partName].Start(c.part_close)
	c.mu.Unlock()

	c.RebalancePtoC() //更新配置
	c.UpdateParts()   //应用配置
	return nil
}

type Consistent struct {
	//排序的hash虚拟节点(环形)
	hashSortedNodes []uint32
	//虚拟节点(consumer)对应的实际节点
	circle map[uint32]string
	//已绑定的consumer为true
	nodes map[string]bool

	//consumer以负责一个Partition为true
	ConH     map[string]bool
	FreeNode int

	mu sync.RWMutex
	//虚拟节点个数
	virtualNodeCount int
}

type PSBConfig_PUSH struct {
	mu sync.RWMutex

	part_close chan *Part
	file       *File

	Cli  *client_operations.Client
	part *Part //
}

func NewPSBConfigPush(in info, file *File, zkclient *zkserver_operations.Client) *PSBConfig_PUSH {
	ret := &PSBConfig_PUSH{
		mu:         sync.RWMutex{},
		part_close: make(chan *Part),
		file:       file,
		part:       NewPart(in, file, zkclient),
	}

	return ret
}
