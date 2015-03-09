/*
Package redisp provides pooled connections to Redis and is built on top of redisb.

The pool is implemented using channels, so requests for a connection from the pool will block until there is one available.

The PDo func wraps the Get, Put, redisb.Do, and Bad funcs.
It will ask for a connection from Get, provide that connection and other args to redisb.Do, eventually calling Put to return the connection.
In the case where the first attempt to call redisb.Do fails with a redisb.ConnError, PDo will return that first connection by calling Bad and then 
wait a random amount of time (no more than that specified when calling redisp.New) before asking the pool for a new
connection to attempt to call redisb.Do a second time.
If the second attempt to call redisb.Do also fails with a redisb.ConnError, then a redisp.PooledConnError is returned.

Typical usage may be as follows:

        package main

        import (
                "github.com/richard-lyman/redisp"
                "net"
                "sync"
                "time"
        )

        func main() {
                creator := func() net.Conn {
                        c, err := net.Dial("tcp", "localhost:6379")
                        if err != nil {
                                panic(err)
                        }
                        return c
                }
                sizeOfPool := 100
                retryDuration := 300*time.Millisecond
                p := redisp.New(sizeOfPool, creator, retryDuration)

                var wg sync.WaitGroup
                for i := 0; i < 10000; i++ {
                        wg.Add(1)
                        go func() {
                                defer wg.Done()
                                _, err := p.PDo("GET", "some_key")
                                if err != nil {
                                        panic(err)
                                }
                        }()
                }
                wg.Wait()
        }

*/
package redisp

import (
	"crypto/rand"
	"fmt"
	"github.com/richard-lyman/redisb"
	"math/big"
	"net"
	"time"
)

// PooledConnError is returned from PDo if two attempts to call redisb.Do both fail with redisb.ConnError
type PooledConnError struct {
	err error
}

func (pce PooledConnError) Error() string {
	return pce.err.Error()
}

// Creator is the function provided in a call to redisp.New for creating the net.Conns that populate the pool
type Creator func() net.Conn

// Pooler defines the Core functionality in this package
type Pooler interface {
        // Get returns a net.Conn from the channel and blocks when there are none available
	Get() net.Conn
        // Put accepts a net.Conn that is placed back on the channel as available
	Put(net.Conn)
        // Bad calls Close on the given net.Conn and creates a new net.Conn that is then made available on the channel
	Bad(net.Conn)
        // PDo is a wrapper around calls to Get, Put, and redisb.Do
        // If the first call to redisb.Do fails, PDo will wait a random amount of time (no more than that provided in the call to redisp.New) before attemping another call to redisb.Do on a new net.Conn
        // If the second call fails, a redisp.PooledConnError will be returned
        // redisp.Bad is used to handle a net.Conn associated with a redisb.ConnError from a call to redisb.Do.
	PDo(...string) (interface{}, error)
}

// New provides an implementation of redisp.Pooler.
// The limit sets the size of the channel.
// The channel is pre-filled with limit amount of calls to c.
// The retryDelay sets the maximum amount of time that PDo will wait before it's second attempt.
func New(limit int, c Creator, retryDelay time.Duration) *Pool {
	ch := make(chan net.Conn, limit)
	for i := 0; i < limit; i++ {
		ch <- c()
	}
	return &Pool{
		creator:    c,
		created:    ch,
		retryDelay: retryDelay,
	}
}

type Pool struct {
	creator    Creator
	created    chan net.Conn
	retryDelay time.Duration
}

func (p *Pool) Get() net.Conn {
	return <-p.created
}

func (p *Pool) Put(c net.Conn) {
	p.created <- c
}

func (p *Pool) Bad(c net.Conn) {
	c.Close()
	p.created <- p.creator()
}

func (p *Pool) PDo(args ...string) (interface{}, error) {
	c := p.Get()
	v, err := redisb.Do(c, args...)
	if _, isConnError := err.(redisb.ConnError); isConnError {
		n, err := rand.Int(rand.Reader, big.NewInt(p.retryDelay.Nanoseconds()))
		if err != nil {
			time.Sleep(p.retryDelay)
		}
		t, err := time.ParseDuration(fmt.Sprintf("%dns", n))
		if err != nil {
			time.Sleep(p.retryDelay)
		}
		time.Sleep(t)
		p.Bad(c)
		c = p.Get()
		v, err = redisb.Do(c, args...)
		if _, isConnError := err.(redisb.ConnError); isConnError {
			p.Bad(c)
			return v, PooledConnError{err}
		}
		p.Put(c)
		return v, err
	}
	p.Put(c)
	return v, err
}
