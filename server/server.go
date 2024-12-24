package main

import (
	"context"
	"crypto/tls"
	"flag"
	example "github.com/rpcxio/rpcx-examples"
	"github.com/smallnest/rpcx/protocol"
	"github.com/smallnest/rpcx/server"
	"net"
	"net/http"
	"time"
)

var (
	addr = flag.String("addr", "localhost:8972", "server address")
)

type Server struct {
	readTimeout  time.Duration //读超时
	writeTimeout time.Duration //写超时
	tlsConfig    *tls.Config   //tls证书
	//Plugins      PluginContainer                                                      //服务器上所有的插件
	AuthFunc func(ctx context.Context, req *protocol.Message, token string) error //检查客户端是否被授权了的鉴权函数
}

// NewServer returns a server
func NewServer(option ...OptionFn) *Server {
	s := &Server{}
	for _, opt := range option {
		opt(s)
	}
	return s
}

func (s *Server) Close() error {

}

func (s *Server) RegisterOnShutdown(f func()) {

}

func (s *Server) Serve(network, address string) (err error) {
	var listener net.Listener
	if listener, err = s.makeListener(network, address); err != nil {

	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {

}

func main() {
	flag.Parse()

	s := server.NewServer()
	s.Register(new(example.Arith), "")
	s.Serve("tcp", *addr)
}
