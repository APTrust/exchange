package network

import (
	"fmt"
	"net"
)

// TCPTestServer is for mocking TCP services in unit tests.
type TCPTestServer struct {
	listener    net.Listener
	isListening bool
}

// NewTCPServer creates a new TCP server.
// Use listenAddress "127.0.0.1:0", then check TCPTestServer.Addr().String()
// to get the address we're listening on. (System assigns port when port is zero.)
func NewTCPTestServer(listenAddress string, callback func(net.Conn)) *TCPTestServer {
	addr, _ := net.ResolveTCPAddr("tcp", listenAddress)
	listener, err := net.Listen("tcp", addr.String())
	if err != nil {
		panic(fmt.Sprintf("Error listening tcp server: %v", err.Error()))
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				panic(fmt.Sprintf("Error accepting tcp connection: %v", err.Error()))
			}
			go callback(conn)
		}
	}()
	return &TCPTestServer{
		listener:    listener,
		isListening: true,
	}
}

func (server *TCPTestServer) Close() {
	server.listener.Close()
	server.isListening = false
}

func (server *TCPTestServer) IsListening() bool {
	return server.isListening
}
