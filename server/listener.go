package main

import (
	"crypto/tls"
	"fmt"
	reuseport "github.com/kavu/go_reuseport"
	"net"
)

type MakeListener func(s *Server, address string) (ln net.Listener, err error)

var makeListeners map[string]MakeListener

func init() {
	makeListeners["tcp"] = tcpMakeListener
	makeListeners["tcp4"] = tcpMakeListener
	makeListeners["tcp6"] = tcpMakeListener
	makeListeners["http"] = tcpMakeListener
	makeListeners["reuseport"] = reuseportMakeListener
	makeListeners["unix"] = unixMakeListener
}

func (s *Server) makeListener(network, address string) (net.Listener, error) {
	ml := makeListeners[network]
	if ml == nil {
		return nil, fmt.Errorf("can not make listener for %s", network)
	}
	return ml(s, address)
}

// 用于是否是一个有效的IPv4地址
func validIP4(address string) bool {
	ip := net.ParseIP(address)
	return ip != nil && ip.To4() != nil
}

func tcpMakeListener(s *Server, address string) (net.Listener, error) {
	if s.tlsConfig == nil {
		return net.Listen("tcp", address)
	} else {
		return tls.Listen("tcp", address, s.tlsConfig)
	}
}

func reuseportMakeListener(s *Server, address string) (net.Listener, error) {
	if validIP4(address) {
		return reuseport.Listen("tcp4", address)
	} else {
		return reuseport.Listen("tcp6", address)
	}
}

func unixMakeListener(s *Server, address string) (net.Listener, error) {
	laddr, err := net.ResolveUnixAddr("unix", address)
	if err != nil {
		return nil, err
	}
	return net.ListenUnix("unix", laddr)
}
