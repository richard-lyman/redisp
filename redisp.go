package redisp

import (
	"crypto/rand"
	"fmt"
	"github.com/richard-lyman/redisb"
	"math/big"
	"net"
	"time"
)

type PooledConnError struct {
	err error
}

func (pce PooledConnError) Error() string {
	return pce.err.Error()
}

type Creator func() net.Conn

type Pool interface {
	Get() net.Conn
	Put(net.Conn)
	Bad(net.Conn)
	PDo(...string) (interface{}, error)
}

func New(limit int, c Creator, retryDelay time.Duration) Pool {
	ch := make(chan net.Conn, limit)
	for i := 0; i < limit; i++ {
		ch <- c()
	}
	return &pool{
		creator:    c,
		created:    ch,
		retryDelay: retryDelay,
	}
}

type pool struct {
	creator    Creator
	created    chan net.Conn
	retryDelay time.Duration
}

func (p *pool) Get() net.Conn {
	return <-p.created
}

func (p *pool) Put(c net.Conn) {
	p.created <- c
}

func (p *pool) Bad(c net.Conn) {
	c.Close()
	p.created <- p.creator()
}

func (p *pool) PDo(args ...string) (interface{}, error) {
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
