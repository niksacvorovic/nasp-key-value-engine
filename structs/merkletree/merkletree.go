package merkletree

import (
	"crypto/md5"
	"encoding/binary"
	"reflect"
)

type MerkleNode struct {
	Hash  []byte
	left  *MerkleNode
	right *MerkleNode
}

type MerkleTree struct {
	MerkleRoot MerkleNode
}

type IncorrentNode struct {
	this  *MerkleNode
	other *MerkleNode
	index int
}

func NewMerkleTree() MerkleTree {
	return MerkleTree{}
}

func (mt *MerkleTree) ConstructMerkleTree(bytes []byte, blockSize int) {
	// Generisanje listova stabla
	leaves := make([]MerkleNode, 0)
	for i := 0; i < len(bytes); i += blockSize {
		chunk := bytes[i:min(i+blockSize, len(bytes)-1)]
		hash := md5.Sum(chunk)
		leaf := MerkleNode{hash[:], nil, nil}
		leaves = append(leaves, leaf)
	}
	if len(leaves)%2 != 0 && len(leaves) != 1 {
		leaves = append(leaves, MerkleNode{make([]byte, 16), nil, nil})
	}
	// Izgradnja viših nivoa stabla
	nextlevel := make([]MerkleNode, 0)
	for i := 1; i < len(leaves); i += 2 {
		newhash := md5.Sum(append(leaves[i-1].Hash, leaves[i].Hash...))
		newnode := MerkleNode{newhash[:], &leaves[i-1], &leaves[i]}
		nextlevel = append(nextlevel, newnode)
	}
	if len(nextlevel)%2 != 0 && len(nextlevel) != 1 {
		nextlevel = append(nextlevel, MerkleNode{make([]byte, 16), nil, nil})
	}
	prevlevel := nextlevel
	for len(prevlevel) > 1 {
		nextlevel = make([]MerkleNode, 0)
		for i := 0; i < len(prevlevel); i += 2 {
			newhash := md5.Sum(append(prevlevel[i].Hash, prevlevel[i+1].Hash...))
			newnode := MerkleNode{newhash[:], &prevlevel[i], &prevlevel[i+1]}
			nextlevel = append(nextlevel, newnode)
		}
		if len(nextlevel)%2 != 0 && len(nextlevel) != 1 {
			nextlevel = append(nextlevel, MerkleNode{make([]byte, 16), nil, nil})
		}
		prevlevel = nextlevel
	}
	mt.MerkleRoot = prevlevel[0]
}

func (mt MerkleTree) Serialize() []byte {
	bytes := make([]byte, 0)
	queue := make([]*MerkleNode, 0)
	counter := make([]byte, 0)
	queue = append(queue, &mt.MerkleRoot)
	counter = append(counter, 0)
	i := 0
	for len(queue) != i || i == 0 {
		bytes = append(bytes, counter[i])
		bytes = append(bytes, queue[i].Hash...)
		if queue[i].left != nil {
			queue = append(queue, queue[i].left)
			counter = append(counter, counter[i]+1)
		}
		if queue[i].right != nil {
			queue = append(queue, queue[i].right)
			counter = append(counter, counter[i]+1)
		}
		i++
	}
	byteCount := binary.BigEndian.AppendUint32([]byte{}, uint32(i))
	bytes = append(byteCount, bytes...)
	return bytes
}

func (mt *MerkleTree) Deserialize(bytes []byte) {
	count := binary.BigEndian.Uint32(bytes[:4])
	var lvlnum byte = 0
	matrix := make([][]MerkleNode, 0)
	matrix = append(matrix, make([]MerkleNode, 0))
	seek := 4
	i := uint32(0)
	for i < count {
		if bytes[seek] > lvlnum {
			lvlnum++
			matrix = append(matrix, make([]MerkleNode, 0))
		}
		seek++
		matrix[lvlnum] = append(matrix[lvlnum], MerkleNode{bytes[seek : seek+16], nil, nil})
		seek += 16
		i++
	}
	for i := range len(matrix) - 1 {
		for j := range matrix[i] {
			if 2*j+2 > len(matrix[i+1]) {
				break
			}
			matrix[i][j].left = &matrix[i+1][2*j]
			matrix[i][j].right = &matrix[i+1][2*j+1]
		}
	}
	mt.MerkleRoot = matrix[0][0]
}

func Compare(this *MerkleTree, other *MerkleTree) (bool, []int) {
	if reflect.DeepEqual(this.MerkleRoot.Hash, other.MerkleRoot.Hash) {
		return true, nil
	}
	incorrectNodes := make([]IncorrentNode, 0)
	current := IncorrentNode{this: &this.MerkleRoot, other: &other.MerkleRoot, index: 0}
	incorrectNodes = append(incorrectNodes, current)
	indices := make([]int, 0)
	i := 0
	for i != len(incorrectNodes) {
		current = incorrectNodes[i]
		if current.this.left == nil && current.other.left == nil && current.this.right == nil && current.other.right == nil {
			indices = append(indices, current.index)
		} else {
			if !reflect.DeepEqual(current.this.left.Hash, current.other.left.Hash) {
				incorrectLeft := IncorrentNode{this: current.this.left, other: current.other.left, index: current.index * 2}
				incorrectNodes = append(incorrectNodes, incorrectLeft)
			}
			if !reflect.DeepEqual(current.this.right.Hash, current.other.right.Hash) {
				incorrectRight := IncorrentNode{this: current.this.right, other: current.other.right, index: current.index*2 + 1}
				incorrectNodes = append(incorrectNodes, incorrectRight)
			}
		}
		i++
	}
	return false, indices
}
