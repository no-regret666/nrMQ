package main

import "net"

type MakeListener func(s *Server, address string) (ln net.Listener, err error)

var makeListeners map[string]MakeListener

func init() {
	makeListeners["tcp"] = make
}

func (s *Server) makeListener(network, address string) (net.Listener, error) {

}
