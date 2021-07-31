package wal

import (
	"container/list"
	"sync"
)

type Cache struct {
	sync.RWMutex
	size int
	l    *list.List
	rmfn func(interface{}) error // call when remove
}

func NewCache(size int, fn func(interface{}) error) *Cache {
	return &Cache{size: size, l: list.New(), rmfn: fn}
}

func (c *Cache) Push(item interface{}) {
	c.Lock()
	defer c.Unlock()
	if c.l.Len() == c.size {
		back := c.l.Back()
		if c.rmfn != nil {
			c.rmfn(back.Value)
		}
		c.l.Remove(back)
	}
	c.l.PushFront(item)
}

func (c *Cache) Range(iter func(value interface{}) bool) {
	c.RLock()
	defer c.RUnlock()
	for i := c.l.Front(); i != nil; i = i.Next() {
		if !iter(i.Value) {
			return
		}
	}
}

func (c *Cache) Clear() {
	c.Lock()
	defer c.Unlock()
	if c.rmfn != nil {
		for i := c.l.Front(); i != nil; i = i.Next() {
			c.rmfn(i.Value)
		}
	}
	c.l = list.New()
}
