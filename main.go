package main

import (
	"fmt"
	"net"
	Server "nrMQ/server"

	"github.com/cloudwego/kitex/server"
)

func main() {
	//start the broker server
	addr, _ := net.ResolveIPAddr("tcp", ":8888")
	var opts []server.Option
	opts = append(opts, server.WithServiceAddr(addr))
	rpcServer := new(Server.RPCServer)

	err := rpcServer.Start(opts)
	if err != nil {
		fmt.Println(err)
	}
}
