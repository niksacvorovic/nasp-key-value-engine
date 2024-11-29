package merkletree

import (
	"crypto/md5"
	"io"
	"os"
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

func GenerateMerkleLeaves(bytes []byte) []MerkleNode {
	leaves := make([]MerkleNode, 0)
	for i := 0; i < len(bytes); i += 4 {
		chunk := make([]byte, 0)
		for j := 0; j < 4; j++ {
			if i+j == len(bytes) {
				break
			}
			chunk = append(chunk, bytes[i+j])
		}
		hash := md5.Sum(chunk)
		leaf := MerkleNode{hash[:], nil, nil}
		leaves = append(leaves, leaf)
	}
	if len(leaves)%2 != 0 {
		leaves = append(leaves, MerkleNode{make([]byte, 0), nil, nil})
	}
	return leaves
}

func ConstructMerkleTree(leaves []MerkleNode) MerkleTree {
	nextlevel := make([]MerkleNode, 0)
	for i := 0; i < len(leaves); i += 2 {
		newhash := md5.Sum(append(leaves[i].Hash, leaves[i+1].Hash...))
		newnode := MerkleNode{newhash[:], &leaves[i], &leaves[i+1]}
		nextlevel = append(nextlevel, newnode)
	}
	if len(nextlevel)%2 != 0 {
		nextlevel = append(nextlevel, MerkleNode{make([]byte, 0), nil, nil})
	}
	prevlevel := nextlevel
	for len(prevlevel) > 1 {
		nextlevel = make([]MerkleNode, 0)
		for i := 0; i < len(prevlevel); i += 2 {
			newhash := md5.Sum(append(prevlevel[i].Hash, prevlevel[i+1].Hash...))
			newnode := MerkleNode{newhash[:], &prevlevel[i], &prevlevel[i+1]}
			nextlevel = append(nextlevel, newnode)
			if len(nextlevel)%2 != 0 && len(nextlevel) != 1 {
				nextlevel = append(nextlevel, MerkleNode{make([]byte, 0), nil, nil})
			}
		}
		prevlevel = nextlevel
	}
	tree := MerkleTree{prevlevel[0]}
	return tree
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
		if len(queue[i].Hash) == 0 {
			bytes = append(bytes, []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}...)
		} else {
			bytes = append(bytes, queue[i].Hash...)
		}
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
	return bytes
}

func (mt *MerkleTree) Deserialize(file *os.File) error {
	lvlnum := make([]byte, 1)
	level := make([]MerkleNode, 0)
	hash := make([]byte, 16)
	matrix := make([][]MerkleNode, 0)
	var current byte = 0
	for {
		// PRAVLJENJE DUBOKE KOPIJE
		hash := append([]byte{}, hash...)
		_, err := file.Read(lvlnum)
		if err != nil {
			panic(err)
		}
		if lvlnum[0] > current {
			matrix = append(matrix, level)
			level = make([]MerkleNode, 0)
			current++
		}
		_, err = file.Read(hash)
		if err == io.EOF {
			matrix = append(matrix, level)
			break
		} else if err != nil {
			panic(err)
		}
		level = append(level, MerkleNode{hash, nil, nil})
	}
	for i := 0; i < len(matrix)-1; i++ {
		for j := 0; j < len(matrix[i]); j++ {
			if 2*j+2 > len(matrix[i+1]) {
				break
			}
			matrix[i][j].left = &matrix[i+1][2*j]
			matrix[i][j].right = &matrix[i+1][2*j+1]
		}
	}
	mt.MerkleRoot = matrix[0][0]
	return nil
}

func Compare(this *MerkleTree, other *MerkleTree) (bool, int) {
	num := 0
	if reflect.DeepEqual(this.MerkleRoot.Hash, other.MerkleRoot.Hash) {
		return true, 0
	}
	currentThis := &this.MerkleRoot
	currentOther := &other.MerkleRoot
	for currentThis.left != nil && currentOther.left != nil && currentThis.right != nil && currentOther.right != nil {
		if !reflect.DeepEqual(currentThis.left.Hash, currentOther.left.Hash) {
			num *= 2
			currentThis = currentThis.left
			currentOther = currentOther.left
		} else if !reflect.DeepEqual(currentThis.right.Hash, currentOther.right.Hash) {
			num *= 2
			num += 1
			currentThis = currentThis.right
			currentOther = currentOther.right
		}
	}
	return false, num
}
