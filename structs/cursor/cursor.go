package cursor

// Struktura cursora
type Cursor interface {
	Seek(seekKey string) bool
	Next() bool
	Key() string
	Value() []byte
	Timestamp() [16]byte
	Tombstone() bool
	Close()
}

// Implementacije se nalaze u hashmap.go/skiplist.go/btree_memtable.go
