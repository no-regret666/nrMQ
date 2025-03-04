package server

import (
	"context"
	"nrMQ/kitex_gen/api"
	"nrMQ/kitex_gen/api/client_operations"
	"os"
	"sync"
	"time"
)

const (
	ALIVE     = "alive"
	DOWN      = "down"
	TOUT      = 60 * 10
	UPDATANUM = 10
)

type Client struct {
	mu       sync.RWMutex
	name     string
	state    string
	consumer client_operations.Client

	subList map[string]*SubScription //若这个consumer关闭，则遍历这些订阅并修改
}

func NewClient(ipport string, con client_operations.Client) *Client {
	client := &Client{
		mu:       sync.RWMutex{},
		name:     ipport,
		consumer: con,
		state:    ALIVE,
		subList:  make(map[string]*SubScription),
	}
	return client
}

func (c *Client) CheckConsumer() bool {
	c.mu = sync.RWMutex{}

	for {
		resp, err := c.consumer.Pingpong(context.Background(), &api.PingPongRequest{
			Ping: true,
		})
		if err != nil || !resp.Pong {
			break
		}

		time.Sleep(time.Second)
	}

	c.mu.Lock()
	c.state = DOWN
	c.mu.Unlock()
	return true
}

type MSGS struct {
	start_index int64
	end_index   int64
	size        int8
	array       []byte //由[]Message转byte
}

type Group struct {
	mu        sync.RWMutex
	topicName string
	consumers map[string]bool //map[client name]alive
}

func NewGroup(topicName, cliName string) *Group {
	group := &Group{
		mu:        sync.RWMutex{},
		topicName: topicName,
		consumers: make(map[string]bool),
	}
	group.consumers[cliName] = true
	return group
}

func (g *Group) DownClient(cliName string) {
	g.mu.Lock()
	_, ok := g.consumers[cliName]
	if ok {
		g.consumers[cliName] = false
	}
	g.mu.Unlock()
}

type Node struct {
	topicName   string
	partName    string
	option      int8
	file        *File
	fd          os.File
	offset      int64
	start_index int64
}

func NewNode(in info, file *File) *Node {
	node := &Node{
		topicName: in.topicName,
		partName:  in.partName,
		option:    in.option,
		file:      file,
	}

	node.fd = *node.file.openFileRead()
	node.offset = -1
	return node
}
