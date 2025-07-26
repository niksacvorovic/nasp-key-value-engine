package containers

import (
	"errors"
	"math/rand"

	"projekat/structs/cursor"
	"projekat/structs/memtable"
)

type Node struct {
	Record memtable.Record
	Next   *Node
	Down   *Node
}

type SkipList struct {
	maxHeight int
	levels    []Node
}

type SkipListMemtable struct {
	data      *SkipList
	watermark uint32
	size      int
	maxSize   int
}

func (s *SkipList) roll() int {
	level := 0
	// possible ret Values from rand are 0 and 1
	// we stop shen we get a 0
	for ; rand.Int31n(2) == 1; level++ {
		if level >= s.maxHeight-1 {
			break
		}
	}
	return level
}

func CreateSL(maxHeight int) *SkipList {
	nodes := make([]Node, maxHeight)
	i := 0
	var downptr *Node
	for i = 0; i < maxHeight; i++ {
		downptr = nil
		if i != 0 {
			downptr = &nodes[i-1]
		}
		nodes[i] = Node{
			Record: memtable.Record{Key: "", Value: []byte{}},
			Next:   nil,
			Down:   downptr,
		}
	}
	return &SkipList{
		maxHeight: maxHeight,
		levels:    nodes,
	}
}

func (sl *SkipList) ReadElement(str string) (memtable.Record, error) {
	current := &sl.levels[sl.maxHeight-1]
	for {
		if current.Record.Key == str {
			break
		}
		if current.Next == nil {
			if current.Down == nil {
				break
			}
			current = current.Down
		} else if current.Next.Record.Key > str {
			if current.Down == nil {
				break
			}
			current = current.Down
		} else {
			current = current.Next
		}
	}
	if current.Record.Key == str {
		return current.Record, nil
	} else {
		return current.Record, errors.New("nonexistent value")
	}

}

func (sl *SkipList) WriteElement(ts [16]byte, tombstone bool, str string, value []byte) bool {
	current := &sl.levels[sl.maxHeight-1]
	stack := make([]*Node, 0)
	var newNode Node
	for {
		if current.Record.Key == str {
			for current != nil {
				current.Record.Value = value
				current.Record.Timestamp = ts
				current.Record.Tombstone = tombstone
				current = current.Down
			}
			return false
		}
		if current.Next == nil {
			if current.Down == nil {
				break
			}
			stack = append(stack, current)
			current = current.Down
		} else if current.Next.Record.Key > str {
			if current.Down == nil {
				break
			}
			stack = append(stack, current)
			current = current.Down
		} else {
			current = current.Next
		}
	}
	newNode = Node{
		Record: memtable.Record{Timestamp: ts, Tombstone: tombstone, Key: str, Value: value},
		Next:   current.Next,
		Down:   nil,
	}
	current.Next = &newNode
	stackLen := len(stack)
	times := sl.roll()
	if times > 0 {
		upperNodes := make([]Node, times)
		bttm := &newNode
		for i := 1; i <= len(upperNodes); i++ {
			upperNodes[i-1] = Node{
				Record: memtable.Record{Timestamp: ts, Tombstone: tombstone, Key: str, Value: value},
				Next:   stack[stackLen-i].Next,
				Down:   bttm,
			}
			stack[stackLen-i].Next = &upperNodes[i-1]
			bttm = &upperNodes[i-1]
		}
	}
	return true
}

func (sl *SkipList) DeleteElement(str string) bool {
	current := &sl.levels[sl.maxHeight-1]
	for {
		if current.Record.Key == str {
			break
		}
		if current.Next == nil {
			if current.Down == nil {
				break
			}
			current = current.Down
		} else if current.Next.Record.Key > str {
			if current.Down == nil {
				break
			}
			current = current.Down
		} else {
			current = current.Next
		}
	}
	if current.Record.Key == str {
		current.Record.Tombstone = true
		current.Record.Value = []byte{}
		for current.Down != nil {
			current = current.Down
			current.Record.Tombstone = true
			current.Record.Value = []byte{}
		}
		return true
	} else {
		return false
	}
}

func NewSkipListMemtable(maxHeight, maxSize int) *SkipListMemtable {
	return &SkipListMemtable{
		data:      CreateSL(maxHeight),
		watermark: 0,
		maxSize:   maxSize,
		size:      0,
	}
}

func (m *SkipListMemtable) Add(ts [16]byte, tombstone bool, key string, value []byte) error {
	if m.size >= m.maxSize {
		return memtable.ErrMemtableFull
	}
	newelem := m.data.WriteElement(ts, tombstone, key, value)
	if newelem {
		m.size++
	}
	return nil
}

func (m *SkipListMemtable) Delete(key string) bool {
	return m.data.DeleteElement(key)
}

func (m *SkipListMemtable) Get(key string) ([]byte, bool, bool) {
	record, exists := m.data.ReadElement(key)
	if exists == nil {
		return record.Value, record.Tombstone, true
	}
	return []byte{}, false, false
}
func (m *SkipListMemtable) Flush() *[]memtable.Record {
	records := make([]memtable.Record, 0, m.size)
	current := &m.data.levels[0]
	for current.Next != nil {
		current = current.Next
		records = append(records, current.Record)
	}
	// Resetovanje Memtabele na početno stanje
	m.data = CreateSL(m.data.maxHeight)
	m.size = 0
	return &records
}

// Isto kao prethodna - treba se izmjeniti
func (m *SkipListMemtable) IsFull() bool {
	return m.size == m.maxSize
}

func (m *SkipListMemtable) SetWatermark(index uint32) {
	m.watermark = max(m.watermark, index)
}

func (m *SkipListMemtable) GetWatermark() uint32 {
	return m.watermark
}

// --------------------------------------------------------------------------------------------------------------------------
// SkipList cursor
// --------------------------------------------------------------------------------------------------------------------------

// SkipList cursor struktura
type SkipListCursor struct {
	head    *Node // pocetni node
	current *Node // Trenutni node
}

// NewCursor pravi cursor koji poceinje na donjem nivou
func (m *SkipListMemtable) NewCursor() cursor.Cursor {
	bottomHead := &m.data.levels[0]
	return &SkipListCursor{
		head:    bottomHead,
		current: bottomHead,
	}
}

// Seek pozicionira cursor na prvi element koji je >= minKey
func (c *SkipListCursor) Seek(minKey string) bool {
	c.current = c.head
	for c.current.Next != nil && c.current.Next.Record.Key < minKey {
		c.current = c.current.Next
	}
	return c.Next()
}

// Next pomjera cursor na sledeci element u donjem nivou
func (c *SkipListCursor) Next() bool {
	if c.current == nil {
		return false
	}
	c.current = c.current.Next
	return c.current != nil
}

// Getter za key
func (c *SkipListCursor) Key() string {
	if c.current == nil {
		return ""
	}
	return c.current.Record.Key
}

// Getter za value
func (c *SkipListCursor) Value() []byte {
	if c.current == nil {
		return nil
	}
	return c.current.Record.Value
}

// Getter za timestamp
func (c *SkipListCursor) Timestamp() [16]byte {
	if c.current == nil {
		return [16]byte{}
	}
	return c.current.Record.Timestamp
}

// Getter za tombstone
func (c *SkipListCursor) Tombstone() bool {
	if c.current == nil {
		return false
	}
	return c.current.Record.Tombstone
}

// Funckija za reset cursora
func (c *SkipListCursor) Close() {
	c.head = nil
	c.current = nil
}
