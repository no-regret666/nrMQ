package client

import (
	"context"
	"fmt"
	"github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/server"
	"net"
	"nrMQ/kitex_gen/api"
	ser "nrMQ/kitex_gen/api/client_operations"
	"nrMQ/kitex_gen/api/server_operations"
	"nrMQ/kitex_gen/api/zkserver_operation"
	"sync"
)

type Consumer struct {
	mu    sync.RWMutex
	Name  string
	State string

	srv      server.Server
	port     string
	zkBroker zkserver_operation.Client
	Brokers  map[string]*server_operations.Client //broker_name--client
}

func NewConsumer(zkBroker string, name string, port string) (*Consumer, error) {
	c := Consumer{
		mu:      sync.RWMutex{},
		Name:    name,
		State:   "alive",
		port:    port,
		Brokers: make(map[string]*server_operations.Client),
	}
	var err error
	fmt.Println("the zkBroker address is ", zkBroker, "the port is ", port)
	c.zkBroker, err = zkserver_operation.NewClient(c.Name, client.WithHostPorts(zkBroker))

	return &c, err
}

func (c *Consumer) Alive() string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.State
}

func (c *Consumer) Start_server() {
	addr, _ := net.ResolveTCPAddr("tcp", c.port)
	var opts []server.Option
	opts = append(opts, server.WithServiceAddr(addr))

	svr := ser.NewServer(c, opts...)
	c.srv = svr
	err := svr.Run()
	if err != nil {
		fmt.Println(err.Error())
	}
}

func (c *Consumer) Shutdown_server() {
	c.srv.Stop()
}

func (c *Consumer) Down() {
	c.mu.Lock()
	c.State = "down"
	c.mu.Unlock()
}

func (c *Consumer) SendInfo(port string, cli *server_operations.Client) error {
	//consumer向broker发送自己的host和ip，使其连接上自己
	resp, err := (*cli).ConInfo(context.Background(), &api.InfoRequest{
		IpPort: port,
	})
	if err != nil {
		fmt.Println(resp)
	}
	return err
}

func (c *Consumer) Subscription(topic, partition string, option int8) (err error) {
	//向zkserver订阅topic和partition
	c.mu.RLock()
	zk := c.zkBroker
	c.mu.RUnlock()

}
