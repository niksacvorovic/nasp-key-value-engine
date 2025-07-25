package wrapper

import (
	"projekat/structs/memtable"
	"projekat/structs/wal"
)

type FullKVImpl struct {
	WAL      *wal.WAL
	Memtable memtable.MemtableInterface
}

func (f *FullKVImpl) Append(tombstone bool, key string, value []byte) ([16]byte, error) {
	return f.WAL.AppendRecord(tombstone, []byte(key), value)
}

func (f *FullKVImpl) Add(ts [16]byte, tombstone bool, key string, value []byte) error {
	return f.Memtable.Add(ts, tombstone, key, value)
}

func (f *FullKVImpl) Get(key string) ([]byte, bool, bool) {
	return f.Memtable.Get(key)
}

func NewFullKV(w *wal.WAL, mt memtable.MemtableInterface) *FullKVImpl {
	return &FullKVImpl{
		WAL:      w,
		Memtable: mt,
	}
}
