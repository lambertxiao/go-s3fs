package freader

import (
	"container/list"
	"sync"
)

type LruCache struct {
	capacity int
	lruList  *list.List
	items    map[string]*list.Element
	lock     sync.RWMutex
}

type Item struct {
	key   string
	value interface{}
}

func newLruCache(capacity int) *LruCache {
	return &LruCache{
		capacity: capacity,
		lruList:  list.New(),
		items:    make(map[string]*list.Element),
	}
}

func (c *LruCache) Get(key string) (value interface{}, ok bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if elem, hit := c.items[key]; hit {
		c.lruList.MoveToFront(elem)
		return elem.Value.(*Item).value, true
	}
	return nil, false
}

func (c *LruCache) Add(key string, value interface{}) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if elem, hit := c.items[key]; hit { // 如果元素存在，则将其移到前面并更新值
		c.lruList.MoveToFront(elem)
		elem.Value.(*Item).value = value
		return
	}

	elem := c.lruList.PushFront(&Item{key, value})
	c.items[key] = elem
}

func (c *LruCache) removeOldest() string {
	elem := c.lruList.Back()
	if elem != nil {
		c.lruList.Remove(elem)
		key := elem.Value.(*Item).key
		delete(c.items, key)
		return key
	}
	return ""
}

func (c *LruCache) isFull() bool {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.lruList.Len() > c.capacity
}

func (c *LruCache) Len() int {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.lruList.Len()
}

func (c *LruCache) Clear() {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.lruList = nil
	c.items = nil
}
