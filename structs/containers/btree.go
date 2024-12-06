package containers

type BTreeNode struct {
	Keys   []string
	Values [][]byte
}

type BTree struct {
	Root BTreeNode
}

func (bt *BTree) ReadElement(key string) ([]byte, error) {
	return []byte{}, nil
}

func (bt *BTree) WriteElement(key string, value []byte) error {
	return nil
}
