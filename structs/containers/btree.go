package containers

import (
	"errors"
	"projekat/structs/memtable"
	//"projekat/structs/memtable"
)

type BTreeNode struct {
	IsLeaf     bool
	Keys       []string
	Values     [][]byte
	Deleted    []bool
	Timestamps [][16]byte
	Children   []*BTreeNode
}

type BTree struct {
	Root   *BTreeNode
	degree int
}

// ---------- KONSTRUKTOR ----------

func NewBTree(degree int) *BTree {
	return &BTree{
		Root: &BTreeNode{
			IsLeaf:     true,
			Keys:       []string{},
			Values:     [][]byte{},
			Deleted:    []bool{},
			Timestamps: [][16]byte{},
			Children:   []*BTreeNode{},
		},
		degree: degree,
	}
}

// ---------- PRETRAGA ----------

func (n *BTreeNode) search(key string) ([]byte, bool, bool) {
	i := 0
	if len(n.Keys) == 0 {
		return nil, false, false
	}
	for i < len(n.Keys) && key > n.Keys[i] {
		i++
	}
	if i < len(n.Keys) && key == n.Keys[i] && !n.Deleted[i] {
		return n.Values[i], n.Deleted[i], true
	}
	if n.IsLeaf {
		return nil, false, false
	}
	return n.Children[i].search(key)
}

func (t *BTree) ReadElement(key string) ([]byte, bool, error) {
	val, del, ok := t.Root.search(key)
	if !ok {
		return nil, del, errors.New("Key not found")
	}
	return val, del, nil
}

// ---------- DODAVANJE ----------

func (t *BTree) WriteElement(key string, value []byte, ts [16]byte, tombstone bool) error {
	if len(t.Root.Keys) == 2*t.degree-1 { //ako je koren pun
		newRoot := &BTreeNode{
			IsLeaf:   false,
			Children: []*BTreeNode{t.Root}, //stari koren je dete novog
		}
		t.splitChild(newRoot, 0)                            //podeli puno dete
		t.insertNonFull(newRoot, key, value, ts, tombstone) //ubacujemo kljuc u novi koren ili potomke
		t.Root = newRoot
	} else {
		t.insertNonFull(t.Root, key, value, ts, tombstone)
	}
	return nil
}

func (t *BTree) insertNonFull(node *BTreeNode, key string, value []byte, ts [16]byte, tombstone bool) {
	i := len(node.Keys) - 1
	//da li kljuc vec postoji u listu
	for j := 0; j < len(node.Keys); j++ {
		if key == node.Keys[j] {
			if node.Deleted[j] && !tombstone { //ako je obrisan, ozivimo ga
				node.Values[j] = value
				node.Deleted[j] = false
				node.Timestamps[j] = ts
			} else if string(node.Values[j]) != string(value) {
				node.Values[j] = value //zamenimo vrednost ako nije ista (URADILA I AZURIRANJE, ne skodi)
				node.Timestamps[j] = ts
			}
			return
		}
	}
	if node.IsLeaf {
		//ubacujemo novi kljuc u list
		node.Keys = append(node.Keys, "")
		node.Values = append(node.Values, nil)
		node.Deleted = append(node.Deleted, false)
		node.Timestamps = append(node.Timestamps, ts)

		for i >= 0 && key < node.Keys[i] { //oslobadjamo mesto za novi kljuc
			node.Keys[i+1] = node.Keys[i]
			node.Values[i+1] = node.Values[i]
			node.Deleted[i+1] = node.Deleted[i]
			node.Timestamps[i+1] = node.Timestamps[i]
			i--
		}
		node.Keys[i+1] = key
		node.Values[i+1] = value
		node.Deleted[i+1] = tombstone
		node.Timestamps[i+1] = ts
	} else { //ako nije list, gledamo u koje dete ulazimo
		for i >= 0 && key < node.Keys[i] {
			i--
		}
		i++

		//ako je dete puno -> ROTACIJA ili SPLIT
		if len(node.Children[i].Keys) == 2*t.degree-1 {
			if !t.tryRotate(node, i) {
				t.splitChild(node, i)
				if key > node.Keys[i] {
					i++
				}
			}
		}
		t.insertNonFull(node.Children[i], key, value, ts, tombstone)
	}
}

// ---------- ROTACIJA ----------

// leva rotacija
func leftRotate(left, parent, child *BTreeNode, i int) {
	// na kraj levog brata postavljamo element sa i-tog mesta
	left.Keys = append(left.Keys, parent.Keys[i-1])
	left.Values = append(left.Values, parent.Values[i-1])
	left.Deleted = append(left.Deleted, parent.Deleted[i-1])
	left.Timestamps = append(left.Timestamps, parent.Timestamps[i-1])
	// na i-to mesto u roditelju stavljamo prvo iz deteta
	parent.Keys[i-1] = child.Keys[0]
	parent.Values[i-1] = child.Values[0]
	parent.Deleted[i-1] = child.Deleted[0]
	parent.Timestamps[i-1] = child.Timestamps[0]
	// sa deteta uklanjamo prvi element
	child.Keys = child.Keys[1:]
	child.Values = child.Values[1:]
	child.Deleted = child.Deleted[1:]
	child.Timestamps = child.Timestamps[1:]
}

// desna rotacija
func rightRotate(right, parent, child *BTreeNode, i int) {
	// na početak desnog brata postavljamo element sa i-tog mesta
	right.Keys = append([]string{parent.Keys[i]}, right.Keys...)
	right.Values = append([][]byte{parent.Values[i]}, right.Values...)
	right.Deleted = append([]bool{parent.Deleted[i]}, right.Deleted...)
	right.Timestamps = append([][16]byte{parent.Timestamps[i]}, right.Timestamps...)
	// na i-to mesto u roditelju stavljamo poslednje iz deteta
	parent.Keys[i] = child.Keys[len(child.Keys)-1]
	parent.Values[i] = child.Values[len(child.Values)-1]
	parent.Deleted[i] = child.Deleted[len(child.Deleted)-1]
	parent.Timestamps[i] = child.Timestamps[len(child.Timestamps)-1]
	// sa deteta uklanjamo poslednji element
	child.Keys = child.Keys[:len(child.Keys)-1]
	child.Values = child.Values[:len(child.Values)-1]
	child.Deleted = child.Deleted[:len(child.Deleted)-1]
	child.Timestamps = child.Timestamps[:len(child.Timestamps)-1]

}

// prvo pokusavamo
func (t *BTree) tryRotate(parent *BTreeNode, i int) bool {
	child := parent.Children[i]
	var left *BTreeNode = nil
	var right *BTreeNode = nil
	if i != 0 {
		left = parent.Children[i-1]
	}
	if i != len(parent.Children)-1 {
		right = parent.Children[i+1]
	}
	if left == nil {
		if right == nil {
			return false
		} else if len(right.Keys) == 2*t.degree-1 {
			return false
		} else {
			rightRotate(right, parent, child, i)
			return true
		}
	}
	if right == nil {
		if len(left.Keys) == 2*t.degree-1 {
			return false
		} else {
			leftRotate(left, parent, child, i)
			return true
		}
	}
	if len(left.Keys) == 2*t.degree-1 && len(right.Keys) == 2*t.degree-1 {
		return false
	}
	if len(left.Keys) <= len(right.Keys) {
		leftRotate(left, parent, child, i)
		return true
	} else {
		rightRotate(right, parent, child, i)
		return true
	}
}

// ---------- SPLIT ----------
//ako rotacija ne uspe delimo

func (t *BTree) splitChild(parent *BTreeNode, i int) {
	full := parent.Children[i]
	mid := t.degree - 1 //indeks srednjeg kljuca koji ide gore u roditalja

	newNode := &BTreeNode{
		IsLeaf:     full.IsLeaf,
		Keys:       append([]string{}, full.Keys[mid+1:]...), //cela desna polovina ide u novi cvor
		Values:     append([][]byte{}, full.Values[mid+1:]...),
		Deleted:    append([]bool{}, full.Deleted[mid+1:]...),
		Timestamps: append([][16]byte{}, full.Timestamps[mid+1:]...),
		Children:   []*BTreeNode{},
	}

	if !full.IsLeaf {
		newNode.Children = append([]*BTreeNode{}, full.Children[mid+1:]...)
		full.Children = full.Children[:mid+1]
	}

	//srednji kljuc se penje u roditelja
	parent.Keys = append(parent.Keys[:i], append([]string{full.Keys[mid]}, parent.Keys[i:]...)...)
	parent.Values = append(parent.Values[:i], append([][]byte{full.Values[mid]}, parent.Values[i:]...)...)
	parent.Deleted = append(parent.Deleted[:i], append([]bool{full.Deleted[mid]}, parent.Deleted[i:]...)...)
	parent.Timestamps = append(parent.Timestamps[:i], append([][16]byte{full.Timestamps[mid]}, parent.Timestamps[i:]...)...)
	parent.Children = append(parent.Children[:i+1], append([]*BTreeNode{newNode}, parent.Children[i+1:]...)...)

	//sad skracujemo levi pun cvor da ima samo levu polovinu kljucva
	full.Keys = full.Keys[:mid]
	full.Values = full.Values[:mid]
	full.Deleted = full.Deleted[:mid]
	full.Timestamps = full.Timestamps[:mid]
}

// ---------- DELETE - logicko ----------

func (t *BTree) MarkAsDeleted(node *BTreeNode, key string) bool {
	i := 0
	for i < len(node.Keys) && key > node.Keys[i] {
		i++
	}
	if i < len(node.Keys) && key == node.Keys[i] {
		if node.Deleted[i] {
			return false
		}
		node.Deleted[i] = true
		return true
	}
	if node.IsLeaf {
		return false
	}
	return t.MarkAsDeleted(node.Children[i], key)
}

// InOrderTraversal vraca sve zapise sortirano
func (t *BTree) InOrderTraversal() []memtable.Record {
	records := make([]memtable.Record, 0)
	t.inOrder(t.Root, &records)
	return records
}

// inOrder je pomocna funkcija za InOrderTraversal i ona obilazi stablo i dodaje zapise u records
func (t *BTree) inOrder(node *BTreeNode, records *[]memtable.Record) {
	if node == nil {
		return
	}
	for i := 0; i < len(node.Keys); i++ {
		if !node.IsLeaf {
			t.inOrder(node.Children[i], records)
		}
		*records = append(*records, memtable.Record{
			Key:       node.Keys[i],
			Value:     node.Values[i],
			Timestamp: node.Timestamps[i],
			Tombstone: node.Deleted[i],
		})
	}
	if !node.IsLeaf && len(node.Children) > len(node.Keys) {
		t.inOrder(node.Children[len(node.Children)-1], records)
	}
}
