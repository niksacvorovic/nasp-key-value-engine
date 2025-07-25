package containers

import (
	//"errors"
	"projekat/structs/cursor"
	"projekat/structs/memtable"
)

type BTreeMemtable struct {
	tree      *BTree
	size      int
	maxSize   int
	watermark uint32
}

func NewBTreeMemtable(maxSize int, degree int) *BTreeMemtable {
	return &BTreeMemtable{
		tree:      NewBTree(degree),
		size:      0,
		maxSize:   maxSize,
		watermark: 0,
	}
}

func (m *BTreeMemtable) Add(ts [16]byte, tombstone bool, key string, value []byte) error {
	_, del, err := m.tree.ReadElement(key)
	isNew := err != nil && !del

	err = m.tree.WriteElement(key, value, ts, tombstone)
	if err != nil {
		return err
	}
	if isNew {
		m.size++
	}
	return nil
}

func (m *BTreeMemtable) Delete(key string) bool {
	return m.tree.MarkAsDeleted(m.tree.Root, key)
}

func (m *BTreeMemtable) Get(key string) ([]byte, bool, bool) {
	val, del, err := m.tree.ReadElement(key)

	return val, del, err == nil
}

func (m *BTreeMemtable) Flush() *[]memtable.Record {
	records := make([]memtable.Record, 0, m.size)
	m.collectRecords(m.tree.Root, &records)
	m.tree = NewBTree(m.tree.degree)
	m.size = 0
	return &records
}

func (m *BTreeMemtable) collectRecords(node *BTreeNode, records *[]memtable.Record) {
	if node == nil {
		return
	}
	for i := 0; i < len(node.Keys); i++ {
		if !node.IsLeaf && i < len(node.Children) {
			m.collectRecords(node.Children[i], records)
		}
		*records = append(*records, memtable.Record{
			Key:       node.Keys[i],
			Value:     node.Values[i],
			Timestamp: node.Timestamps[i],
			Tombstone: node.Deleted[i],
		})
	}
	if !node.IsLeaf && len(node.Children) > len(node.Keys) {
		m.collectRecords(node.Children[len(node.Children)-1], records)
	}
}

func (m *BTreeMemtable) SetWatermark(index uint32) {
	if index > m.watermark {
		m.watermark = index
	}
}

func (m *BTreeMemtable) GetWatermark() uint32 {
	return m.watermark
}

func (m *BTreeMemtable) IsFull() bool {
	return m.size >= m.maxSize
}

// --------------------------------------------------------------------------------------------------------------------------
// BTree cursor
// --------------------------------------------------------------------------------------------------------------------------

// BTree cursor struktura
type BTreeCursor struct {
	records []memtable.Record // Zapisi
	current int               // Trenutni
}

// NewCursor pravi cursor sa svim zapisima sortiranim
func (m *BTreeMemtable) NewCursor() cursor.Cursor {
	records := m.tree.InOrderTraversal()
	return &BTreeCursor{
		records: records,
		current: -1,
	}
}

// Seek pozicionira cursor na prvi kljuc koji je >= minKey
func (c *BTreeCursor) Seek(minKey string) bool {
	// Binarna pretraga za prvi kljuc koji je >= minKey
	left, right := 0, len(c.records)
	for left < right {
		mid := left + (right-left)/2
		if c.records[mid].Key < minKey {
			left = mid + 1
		} else {
			right = mid
		}
	}
	if left < len(c.records) {
		c.current = left
		return true
	}

	// Nije pronadjen kljuc koji je >= minKey
	c.current = len(c.records)
	return false
}

// Next pomjera cursor na sledeci element
func (c *BTreeCursor) Next() bool {
	if c.current < 0 {
		if len(c.records) == 0 {
			return false
		}
		c.current = 0
		return true
	}

	c.current++
	return c.current < len(c.records)
}

// Getter za key
func (c *BTreeCursor) Key() string {
	if c.current < 0 || c.current >= len(c.records) {
		return ""
	}
	return c.records[c.current].Key
}

// Getter za value
func (c *BTreeCursor) Value() []byte {
	if c.current < 0 || c.current >= len(c.records) {
		return nil
	}
	return c.records[c.current].Value
}

// Getter za timestamp
func (c *BTreeCursor) Timestamp() [16]byte {
	if c.current < 0 || c.current >= len(c.records) {
		return [16]byte{}
	}
	return c.records[c.current].Timestamp
}

// Getter za tombstone
func (c *BTreeCursor) Tombstone() bool {
	if c.current < 0 || c.current >= len(c.records) {
		return false
	}
	return c.records[c.current].Tombstone
}

// Funckija za reset cursora
func (c *BTreeCursor) Close() {
	c.records = nil
	c.current = -1
}
