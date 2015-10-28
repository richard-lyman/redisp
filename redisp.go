/*
Package redisp provides pooled connections to Redis and is built on top of redisb.

The pool is implemented using channels, so requests for a connection from the pool will block until there is one available.

The Do func wraps the Get, Put, redisb.Do, and Bad funcs.
It will ask for a connection from Get, provide that connection and other args to redisb.Do, eventually calling Put to return the connection.
In the case where the first attempt to call redisb.Do fails with a redisb.ConnError, Do will return that first connection by calling Bad and then
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
                                _, err := p.Do("GET", "some_key")
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

// PooledConnError is returned from Do if two attempts to call redisb.Do both fail with redisb.ConnError
type PooledConnError struct {
	err error
}

func (pce PooledConnError) Error() string {
	return pce.err.Error()
}

// Creator is the function provided in a call to redisp.New for creating the net.Conns that populate the pool
type Creator func() net.Conn

/*
// Pooler defines the Core functionality in this package
type Pooler interface {
	Emtpy()
	Fill()
	// Get returns a net.Conn from the channel and blocks when there are none available
	Get() net.Conn
	// Put accepts a net.Conn that is placed back on the channel as available
	Put(net.Conn)
	// Bad calls Close on the given net.Conn and creates a new net.Conn that is then made available on the channel
	Bad(net.Conn)
	// Do is a wrapper around calls to Get, Put, and redisb.Do
	// If the first call to redisb.Do fails, Do will wait a random amount of time (no more than that provided in the call to redisp.New) before attemping another call to redisb.Do on a new net.Conn
	// If the second call fails, a redisp.PooledConnError will be returned
	// redisp.Bad is used to handle a net.Conn associated with a redisb.ConnError from a call to redisb.Do.
	Do(...string) (interface{}, error)
}
*/

// The limit sets the size of the channel.
// The channel is pre-filled with limit amount of calls to c.
// The retryDelay sets the maximum amount of time that Do will wait before it's second attempt.
func New(limit int, creator Creator, retryDelay time.Duration) *Pool {
	p := &Pool{
		limit:      limit,
		creator:    creator,
		retryDelay: retryDelay,
		tracked:    []net.Conn{},
		created:    make(chan net.Conn, limit),
	}
	p.Fill()
	return p
}

type Pool struct {
	limit      int
	tracked    []net.Conn
	creator    Creator
	created    chan net.Conn
	retryDelay time.Duration
}

/*
Empty will force a call to Close on all net.Conns and remove all available net.Conns from the pool.

You will need to call Fill to unblock calls to Get.
*/
func (p *Pool) Empty() {
	go func(p *Pool) {
		for {
			if len(p.created) == 0 {
				break
			}
			<-p.created
		}
		for _, c := range p.tracked {
			c.Close() // TODO - it might be nice to know that close was called on a Conn and block it from being re-added to the pool
		}
	}(p)
	p.tracked = make([]net.Conn, p.limit)
}

// Fill will add net.Conns to the pool by calling the Creator provided in New, until the number of net.Conns is equal to the limit given in New
func (p *Pool) Fill() {
	if len(p.tracked) == p.limit {
		return
	}
	for i := len(p.tracked); i < p.limit; i++ {
		tmp := p.creator()
		p.tracked = append(p.tracked, tmp)
		p.created <- tmp
	}
}

// Get takes one net.Conn off of the internal chan net.Conn - so it blocks if there are no available net.Conns
func (p *Pool) Get() net.Conn {
	return <-p.created
}

// Put returns a net.Conn to the internal chan net.Conn
func (p *Pool) Put(c net.Conn) {
	p.created <- c
}

func (p *Pool) removeFromTracked(c net.Conn) {
	for i, v := range p.tracked {
		if v == c {
			p.tracked = append(p.tracked[:i], p.tracked[i+1:]...)
			break
		}
	}
}

// Bad calls Close on the net.Conn and then adds a new net.Conn to the internal chan net.Conn by calling the creator given in New
func (p *Pool) Bad(c net.Conn) {
	p.removeFromTracked(c)
	c.Close()
	p.created <- p.creator()
}

/*
Do wraps the calls to Get, Bad, Put, and redisb.Do, with an internal second attempt to call redisb.Do if the first fails from a redisb.ConnError.

The delay before the second attempt to call redisb.Do is based on a random time.Duration no greater than the retryDelay given in New.
*/
func (p *Pool) Do(args ...string) (interface{}, error) {
	c := p.Get()
	v, err := redisb.Do(c, args...)
	if _, isConnError := err.(redisb.ConnError); isConnError {
		n, err := rand.Int(rand.Reader, big.NewInt(p.retryDelay.Nanoseconds()))
		if err != nil {
			time.Sleep(p.retryDelay)
		} else {
			t, err := time.ParseDuration(fmt.Sprintf("%dns", n))
			if err != nil {
				time.Sleep(p.retryDelay)
			} else {
				time.Sleep(t)
			}
		}
		p.Bad(c)
		c = p.Get()
		v, err = redisb.Do(c, args...)
		if _, isConnError := err.(redisb.ConnError); isConnError {
			p.Bad(c)
			return nil, PooledConnError{err}
		}
		p.Put(c)
		return v, err
	}
	p.Put(c)
	return v, err
}
