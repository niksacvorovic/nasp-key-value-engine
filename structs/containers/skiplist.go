package containers

import (
	"errors"
	"math/rand"
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

func (s *SkipList) roll() int {
	level := 0
	// possible ret Values from rand are 0 and 1
	// we stop shen we get a 0
	for ; rand.Int31n(2) == 1; level++ {
		if level >= s.maxHeight {
			return level - 1
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

func (sl *SkipList) WriteElement(str string, value []byte) error {
	current := &sl.levels[sl.maxHeight-1]
	stack := make([]*Node, 0)
	var newNode Node
	for {
		if current.Key == str {
			return errors.New("duplicate element")
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
		for i := 0; i < len(upperNodes); i++ {
			upperNodes[i] = Node{
				Key:   str,
				Value: value,
				Next:  stack[stackLen-1-i].Next,
				Down:  bttm,
			}
			stack[stackLen-1-i].Next = &upperNodes[i]
			bttm = &upperNodes[i]
		}
	}
	return nil
}
