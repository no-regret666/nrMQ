package main

import (
	"context"
	"flag"
	example "github.com/rpcxio/rpcx-examples"
	"github.com/smallnest/rpcx/protocol"
	"github.com/smallnest/rpcx/server"
	"net/http"
)

var (
	addr = flag.String("addr", "localhost:8972", "server address")
)

type Server struct {
	Plugins  PluginContainer
	AuthFunc func(ctx context.Context, req *protocol.Message, token string) error
}

func NewServer() *Server {
	return &Server{}
}

func (s *Server) Close() error {

}

func (s *Server) RegisterOnShutdown(f func()) {

}

func (s *Server) Serve(network, address string) (err error) {

}

func (s *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {

}

func main() {
	flag.Parse()

	s := server.NewServer()
	s.Register(new(example.Arith), "")
	s.Serve("tcp", *addr)
}
