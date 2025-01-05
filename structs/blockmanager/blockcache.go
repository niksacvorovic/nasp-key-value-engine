package blockmanager

type Signature struct {
	path   string
	number int
}

type BlockNode struct {
	data      []byte
	blocksign Signature
	prev      *BlockNode
	next      *BlockNode
}

type BlockCache struct {
	first    *BlockNode
	last     *BlockNode
	hash     map[Signature]*BlockNode
	length   int
	capacity int
}

func NewBlockCache(cap int) *BlockCache {
	bc := BlockCache{nil, nil, make(map[Signature]*BlockNode), 0, cap}
	return &bc
}

func (bc *BlockCache) AddToCache(path string, number int, data []byte) {
	sign := Signature{path, number}
	node := BlockNode{data, sign, bc.first, nil}
	if bc.length == 0 {
		bc.last = &node
	} else {
		bc.first.next = &node
	}
	bc.first = &node
	bc.hash[sign] = &node
	if bc.length == bc.capacity {
		bc.last = bc.last.next
		delete(bc.hash, bc.last.prev.blocksign)
		bc.last.prev = nil
	} else {
		bc.length++
	}
}

func (bc *BlockCache) FindInCache(path string, number int) ([]byte, bool) {
	sign := Signature{path, number}
	block, ok := bc.hash[sign]
	if !ok {
		return []byte{}, ok
	} else {
		if block.next != nil {
			block.next.prev = block.prev
		}
		if block.prev != nil {
			block.prev.next = block.next
		}
		block.next = nil
		if bc.first != block {
			block.prev = bc.first
			bc.first.next = block
			bc.first = block
		}
		return block.data, ok
	}
}
