package server

import (
	"nrMQ/kitex_gen/api/client_operations"
	"sync"
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
