package containers

import (
	"errors"
	"fmt"
	"math/rand"
	"os"

	"projekat/structs/blockmanager"
	"projekat/structs/memtable"
)

type Node struct {
	Key   string
	Value []byte
	Next  *Node
	Down  *Node
}

type SkipList struct {
	maxHeight int
	levels    []Node
}

type SkipListMemtable struct {
	data         *SkipList
	size         int
	maxSize      int
	blockManager *blockmanager.BlockManager
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
			Key:  "",
			Next: nil,
			Down: downptr,
		}
	}
	return &SkipList{
		maxHeight: maxHeight,
		levels:    nodes,
	}
}

func (sl *SkipList) ReadElement(str string) ([]byte, error) {
	current := &sl.levels[sl.maxHeight-1]
	for {
		if current.Key == str {
			break
		}
		if current.Next == nil {
			if current.Down == nil {
				break
			}
			current = current.Down
		} else if current.Next.Key > str {
			if current.Down == nil {
				break
			}
			current = current.Down
		} else {
			current = current.Next
		}
	}
	if current.Key == str {
		return current.Value, nil
	} else {
		return current.Value, errors.New("nonexistent value")
	}

}

func (sl *SkipList) WriteElement(str string, value []byte) bool {
	current := &sl.levels[sl.maxHeight-1]
	stack := make([]*Node, 0)
	var newNode Node
	for {
		if current.Key == str {
			for current != nil {
				current.Value = value
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
		} else if current.Next.Key > str {
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
		Key:   str,
		Value: value,
		Next:  current.Next,
		Down:  nil,
	}
	current.Next = &newNode
	stackLen := len(stack)
	times := sl.roll()
	if times > 0 {
		upperNodes := make([]Node, times)
		bttm := &newNode
		for i := 1; i <= len(upperNodes); i++ {
			upperNodes[i-1] = Node{
				Key:   str,
				Value: value,
				Next:  stack[stackLen-i].Next,
				Down:  bttm,
			}
			stack[stackLen-i].Next = &upperNodes[i-1]
			bttm = &upperNodes[i-1]
		}
	}
	return true
}

func (sl *SkipList) DeleteElement(str string) error {
	current := &sl.levels[sl.maxHeight-1]
	for {
		if current.Key == str {
			break
		}
		if current.Next == nil {
			if current.Down == nil {
				break
			}
			current = current.Down
		} else if current.Next.Key > str {
			if current.Down == nil {
				break
			}
			current = current.Down
		} else {
			current = current.Next
		}
	}
	if current.Key == str {
		current.Value = []byte{}
		for current.Down != nil {
			current = current.Down
			current.Value = []byte{}
		}
		return nil
	} else {
		return errors.New("nonexistent value")
	}
}

func NewSkipListMemtable(maxHeight, maxSize int, blockManager *blockmanager.BlockManager) *SkipListMemtable {
	return &SkipListMemtable{
		data:         CreateSL(maxHeight),
		maxSize:      maxSize,
		size:         0,
		blockManager: blockManager,
	}
}

func (m *SkipListMemtable) Add(key string, value []byte) error {
	if m.size >= m.maxSize {
		return memtable.MemtableFull
	}
	newelem := m.data.WriteElement(key, value)
	if newelem {
		m.size++
	}
	return nil
}

func (m *SkipListMemtable) Delete(key string) error {
	err := m.data.DeleteElement(key)
	if err != nil {
		return err
	}
	return nil
}

func (m *SkipListMemtable) Get(key string) ([]byte, bool) {
	value, exists := m.data.ReadElement(key)
	if exists == nil {
		return value, true
	}
	return []byte{}, false
}

func (m *SkipListMemtable) LoadFromWAL(file *os.File, offset int64) (int64, error) {
	return memtable.LoadFromWALHelper(file, m, offset)
}

func (m *SkipListMemtable) Serialize() error {
	blockData := make([]byte, 0, m.maxSize*10)
	current := &m.data.levels[m.data.maxHeight-1]
	for current.Next != nil {
		current = current.Next
		keyLen := len(current.Key)
		valueLen := len(current.Value)
		blockData = append(blockData, byte(keyLen))
		blockData = append(blockData, []byte(current.Key)...)
		blockData = append(blockData, byte(valueLen))
		blockData = append(blockData, current.Value...)
	}
	blockData = append(blockData, make([]byte, 4096-len(blockData))...)
	err := m.blockManager.WriteBlock("file.data", 0, blockData)
	if err != nil {
		return fmt.Errorf("error writing to BlockManager: %v", err)
	}
	return nil
}
