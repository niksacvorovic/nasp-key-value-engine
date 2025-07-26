package lrucache

type CacheNode struct {
	key   string
	value []byte
	next  *CacheNode
	prev  *CacheNode
}

type LRUCache struct {
	hash     map[string]*CacheNode
	head     *CacheNode
	tail     *CacheNode
	length   int
	capacity int
}

func NewLRUCache(capacity int) LRUCache {
	head := CacheNode{"", []byte{}, nil, nil}
	tail := CacheNode{key: " ", value: []byte{}, next: &head, prev: nil}
	head.prev = &tail
	return LRUCache{make(map[string]*CacheNode), &head, &tail, 0, capacity}
}

func (cache *LRUCache) CheckCache(key string) ([]byte, bool) {
	node, ok := cache.hash[key]
	if ok {
		node.prev.next = node.next
		node.next.prev = node.prev
		node.next = cache.head
		node.prev = cache.head.prev
		cache.head.prev.next = node
		cache.head.prev = node
		return node.value, true
	}
	return nil, false
}

func (cache *LRUCache) UpdateCache(key string, value []byte) {
	node, exists := cache.hash[key]
	if exists {
		node.prev.next = node.next
		node.next.prev = node.prev
		node.next = cache.head
		node.prev = cache.head.prev
		cache.head.prev.next = node
		cache.head.prev = node
		node.value = value
	} else {
		newnode := CacheNode{key, value, cache.head, cache.head.prev}
		cache.hash[key] = &newnode
		cache.head.prev.next = &newnode
		cache.head.prev = &newnode
		if cache.length == cache.capacity {
			cache.tail.next.next.prev = cache.tail
			delete(cache.hash, cache.tail.next.key)
			newnext := cache.tail.next.next
			cache.tail.next.next = nil
			cache.tail.next.prev = nil
			cache.tail.next = newnext
		} else {
			cache.length++
		}
	}
}

func (cache *LRUCache) DeleteFromCache(key string) {
	node, exists := cache.hash[key]
	if exists {
		node.next.prev = node.prev
		node.prev.next = node.next
		node.next = nil
		node.prev = nil
		delete(cache.hash, node.key)
		cache.length--
	}
}
