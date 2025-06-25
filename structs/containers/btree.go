package containers

import (
	"errors"
	//"projekat/structs/memtable"
)

const degree = 3

type BTreeNode struct {
	IsLeaf   bool
	Keys     []string
	Values   [][]byte
	Deleted  []bool
	Children []*BTreeNode
}

type BTree struct {
	Root *BTreeNode
}

// ---------- KONSTRUKTOR ----------

func NewBTree() *BTree {
	return &BTree{
		Root: &BTreeNode{
			IsLeaf:   true,
			Keys:     []string{},
			Values:   [][]byte{},
			Deleted:  []bool{},
			Children: []*BTreeNode{},
		},
	}
}

// ---------- PRETRAGA ----------

func (n *BTreeNode) search(key string) ([]byte, bool) {
	i := 0
	for i < len(n.Keys) && key > n.Keys[i] {
		i++
	}
	if i < len(n.Keys) && key == n.Keys[i] && !n.Deleted[i] {
		return n.Values[i], true
	}
	if n.IsLeaf {
		return nil, false
	}
	return n.Children[i].search(key)
}

func (t *BTree) ReadElement(key string) ([]byte, error) {
	val, ok := t.Root.search(key)
	if !ok {
		return nil, errors.New("Key not found")
	}
	return val, nil
}

// ---------- DODAVANJE ----------

func (t *BTree) WriteElement(key string, value []byte) error {
	if len(t.Root.Keys) == 2*degree-1 { //ako je koren pun
		newRoot := &BTreeNode{
			IsLeaf:   false,
			Children: []*BTreeNode{t.Root}, //stari koren je dete novog
		}
		t.splitChild(newRoot, 0)             //podeli puno dete
		t.insertNonFull(newRoot, key, value) //ubacujemo kljuc u novi koren ili potomke
		t.Root = newRoot
	} else {
		t.insertNonFull(t.Root, key, value)
	}
	return nil
}

func (t *BTree) insertNonFull(node *BTreeNode, key string, value []byte) {
	i := len(node.Keys) - 1

	if node.IsLeaf { //da li kljuc vec postoji u listu
		for j := 0; j < len(node.Keys); j++ {
			if key == node.Keys[j] {
				if node.Deleted[j] { //ako je obrisan, ozivimo ga
					node.Values[j] = value
					node.Deleted[j] = false
				} else if string(node.Values[j]) != string(value) {
					node.Values[j] = value //zamenimo vrednost ako nije ista (URADILA I AZURIRANJE, ne skodi)
				}
				return
			}
		}
		//ubacujemo novi kljuc u list
		node.Keys = append(node.Keys, "")
		node.Values = append(node.Values, nil)
		node.Deleted = append(node.Deleted, false)

		for i >= 0 && key < node.Keys[i] { //oslobadjamo mesto za novi kljuc
			node.Keys[i+1] = node.Keys[i]
			node.Values[i+1] = node.Values[i]
			node.Deleted[i+1] = node.Deleted[i]
			i--
		}
		node.Keys[i+1] = key
		node.Values[i+1] = value
		node.Deleted[i+1] = false
	} else { //ako nije list, gledamo u koje dete ulazimo
		for i >= 0 && key < node.Keys[i] {
			i--
		}
		i++

		//ako je dete puno -> ROTACIJA ili SPLIT
		if len(node.Children[i].Keys) == 2*degree-1 {
			if !t.tryRotate(node, i) {
				t.splitChild(node, i)
				if key > node.Keys[i] {
					i++
				}
			}
		}
		t.insertNonFull(node.Children[i], key, value)
	}
}

// ---------- ROTACIJA ----------
//prvo pokusavamo

func (t *BTree) tryRotate(parent *BTreeNode, i int) bool {
	child := parent.Children[i]

	//leva rotacija, koristimo mesto u LEVOM BRATU
	if i > 0 {
		left := parent.Children[i-1]
		if len(left.Keys) < 2*degree-1 {
			//pomeri kljuc iz roditelja u dete a roditeljski kljuc popuni sa desnim kljucem iz leve brace
			child.Keys = append([]string{parent.Keys[i-1]}, child.Keys...)
			child.Values = append([][]byte{parent.Values[i-1]}, child.Values...)
			child.Deleted = append([]bool{parent.Deleted[i-1]}, child.Deleted...)

			idx := len(left.Keys) - 1
			parent.Keys[i-1] = left.Keys[idx]
			parent.Values[i-1] = left.Values[idx]
			parent.Deleted[i-1] = left.Deleted[idx]

			left.Keys = left.Keys[:idx]
			left.Values = left.Values[:idx]
			left.Deleted = left.Deleted[:idx]

			if !child.IsLeaf { //ako dete nije list, pomeri i decu sa leve strane
				child.Children = append([]*BTreeNode{left.Children[idx+1]}, child.Children...)
				left.Children = left.Children[:idx+1]
			}
			return true
		}
	}

	//desna rotacija, koristimo mesto u DESNOM BRATU
	if i < len(parent.Children)-1 {
		right := parent.Children[i+1]
		if len(right.Keys) < 2*degree-1 {
			child.Keys = append(child.Keys, parent.Keys[i])
			child.Values = append(child.Values, parent.Values[i])
			child.Deleted = append(child.Deleted, parent.Deleted[i])

			parent.Keys[i] = right.Keys[0]
			parent.Values[i] = right.Values[0]
			parent.Deleted[i] = right.Deleted[0]

			right.Keys = right.Keys[1:]
			right.Values = right.Values[1:]
			right.Deleted = right.Deleted[1:]

			if !child.IsLeaf {
				child.Children = append(child.Children, right.Children[0])
				right.Children = right.Children[1:]
			}
			return true
		}
	}

	return false
}

// ---------- SPLIT ----------
//ako rotacija ne uspe delimo

func (t *BTree) splitChild(parent *BTreeNode, i int) {
	full := parent.Children[i]
	mid := degree - 1 //indeks srednjeg kljuca koji ide gore u roditalja

	newNode := &BTreeNode{
		IsLeaf:   full.IsLeaf,
		Keys:     append([]string{}, full.Keys[mid+1:]...), //cela desna polovina ide u novi cvor
		Values:   append([][]byte{}, full.Values[mid+1:]...),
		Deleted:  append([]bool{}, full.Deleted[mid+1:]...),
		Children: []*BTreeNode{},
	}

	if !full.IsLeaf {
		newNode.Children = append([]*BTreeNode{}, full.Children[mid+1:]...)
		full.Children = full.Children[:mid+1]
	}

	//srednji kljuc se penje u roditelja
	parent.Keys = append(parent.Keys[:i], append([]string{full.Keys[mid]}, parent.Keys[i:]...)...)
	parent.Values = append(parent.Values[:i], append([][]byte{full.Values[mid]}, parent.Values[i:]...)...)
	parent.Deleted = append(parent.Deleted[:i], append([]bool{full.Deleted[mid]}, parent.Deleted[i:]...)...)
	parent.Children = append(parent.Children[:i+1], append([]*BTreeNode{newNode}, parent.Children[i+1:]...)...)

	//sad skracujemo levi pun cvor da ima samo levu polovinu kljucva
	full.Keys = full.Keys[:mid]
	full.Values = full.Values[:mid]
	full.Deleted = full.Deleted[:mid]
}

// ---------- DELETE - logicko ----------

func (t *BTree) DeleteElement(key string) error {
	return t.markAsDeleted(t.Root, key)
}

func (t *BTree) markAsDeleted(node *BTreeNode, key string) error {
	i := 0
	for i < len(node.Keys) && key > node.Keys[i] {
		i++
	}
	if i < len(node.Keys) && key == node.Keys[i] {
		if node.Deleted[i] {
			return errors.New("Already deleted")
		}
		node.Deleted[i] = true
		return nil
	}
	if node.IsLeaf {
		return errors.New("Key not found")
	}
	return t.markAsDeleted(node.Children[i], key)
}
