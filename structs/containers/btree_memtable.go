package containers

import (
	"math"
	//"errors"
	"projekat/structs/memtable"
)

type BTreeMemtable struct {
	tree          *BTree
	size          int
	maxSize       int
	lowWatermark  uint32
	highWatermark uint32
}

func NewBTreeMemtable(maxSize int) *BTreeMemtable {
	return &BTreeMemtable{
		tree:          NewBTree(),
		size:          0,
		maxSize:       maxSize,
		lowWatermark:  math.MaxUint32,
		highWatermark: 0,
	}
}

func (m *BTreeMemtable) Add(ts [16]byte, tombstone bool, key string, value []byte) error {
	if m.IsFull() {
		return memtable.ErrMemtableFull
	}
	if tombstone {
		return m.tree.DeleteElement(key)
	}
	//ako postoji ne povecavamo size
	_, err := m.tree.ReadElement(key)
	isNew := err != nil

	err = m.tree.WriteElement(key, value)
	if err != nil {
		return err
	}
	if isNew {
		m.size++
	}
	return nil
}

func (m *BTreeMemtable) Delete(key string) error {
	return m.tree.DeleteElement(key)
}

func (m *BTreeMemtable) Get(key string) ([]byte, bool) {
	val, err := m.tree.ReadElement(key)
	if err != nil {
		return []byte{}, false
	}
	return val, true
}

func (m *BTreeMemtable) Flush() *[]memtable.Record {
	records := make([]memtable.Record, 0, m.size)
	m.collectRecords(m.tree.Root, &records)
	m.tree = NewBTree()
	m.size = 0
	return &records
}

func (m *BTreeMemtable) collectRecords(node *BTreeNode, records *[]memtable.Record) {
	if node == nil {
		return
	}
	for i := 0; i < len(node.Keys); i++ {
		if !node.Deleted[i] {
			*records = append(*records, memtable.Record{
				Key:       node.Keys[i],
				Value:     node.Values[i],
				Timestamp: [16]byte{},
				Tombstone: false,
			})
		}
		if !node.IsLeaf && i < len(node.Children) {
			m.collectRecords(node.Children[i], records)
		}
	}
	if !node.IsLeaf && len(node.Children) > len(node.Keys) {
		m.collectRecords(node.Children[len(node.Children)-1], records)
	}
}

func (m *BTreeMemtable) SetWatermarks(index uint32) {
	if index < m.lowWatermark {
		m.lowWatermark = index
	}
	if index > m.highWatermark {
		m.highWatermark = index
	}
}

func (m *BTreeMemtable) GetWatermarks() (uint32, uint32) {
	return m.lowWatermark, m.highWatermark
}

func (m *BTreeMemtable) IsFull() bool {
	return m.size >= m.maxSize
}
