package rpcconn

import (
	"io"
	"net"
	"net/rpc"
	"sync"
)

type Client struct {
	network string
	dialer  Address

	mu *sync.RWMutex
	c  *rpc.Client
}

type Address interface {
	String() string
}

func DialHTTP(network string, dialer Address) (*Client, error) {
	client, err := rpc.DialHTTP(network, dialer.String())
	if err != nil {
		return nil, err
	}

	return &Client{
		network: network,
		dialer:  dialer,
		mu:      new(sync.RWMutex),
		c:       client,
	}, nil
}

func DialHTTPPath(network string, dialer Address, path string) (*Client, error) {
	client, err := rpc.DialHTTPPath(network, dialer.String(), path)
	if err != nil {
		return nil, err
	}

	return &Client{
		network: network,
		dialer:  dialer,
		mu:      new(sync.RWMutex),
		c:       client,
	}, nil
}

func (client *Client) Close() error {
	if client == nil {
		return nil
	}

	client.mu.Lock()
	defer client.mu.Unlock()

	return client.c.Close()
}

func (client *Client) Call(method string, args, reply interface{}) error {
	if client == nil {
		return nil
	}

	client.mu.RLock()

	err := client.c.Call(method, args, reply)
	if err != nil && IsNetworkError(err) {
		client.mu.RUnlock()
		client.Reconnect()
		//retry call
		return client.Call(method, args, reply)
	}

	client.mu.RUnlock()

	return err
}

func (client *Client) Reconnect() {
	var rpcClient *rpc.Client
	var err error

	client.mu.Lock()
	for {
		rpcClient, err = rpc.DialHTTP(client.network, client.dialer.String())
		if err == nil {
			break
		}
	}
	client.c = rpcClient
	client.mu.Unlock()
}

func (client *Client) Go(method string, args, reply interface{}, done chan *rpc.Call) *rpc.Call {
	if client == nil {
		return nil
	}

	client.mu.RLock()
	defer client.mu.RUnlock()

	return client.c.Go(method, args, reply, done)
}

func IsNetworkError(err error) bool {
	_, ok := err.(*net.OpError)
	return ok || err == io.ErrUnexpectedEOF || err == rpc.ErrShutdown
}
