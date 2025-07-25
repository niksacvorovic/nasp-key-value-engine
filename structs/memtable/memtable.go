package memtable

import (
	"errors"

	"projekat/structs/cursor"
	"projekat/structs/sstable"
)

// Jedan zapis ključ - vrednost
type Record struct {
	Timestamp [16]byte
	Tombstone bool
	Key       string
	Value     []byte
}

// MemtableInterface definise zajednicki interfejs za sve implementacije Memtable-a
type MemtableInterface interface {
	Add(ts [16]byte, tombstone bool, key string, value []byte) error
	Delete(key string) bool
	Get(key string) ([]byte, bool, bool)
	SetWatermark(index uint32)
	GetWatermark() uint32
	Flush() *[]Record
	IsFull() bool
	NewCursor() cursor.Cursor
}

// Greška ukoliko je Memtable popunjen
var ErrMemtableFull error = errors.New("memtable full")

// Utility funkcije za konverziju tipova
func TimestampToBytes(ts [16]byte) []byte {
	return ts[:]
}

func BoolToByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

// ConvertMemToSST konvertuje podatke iz Memtable u SSTable format
func ConvertMemToSST(mt *MemtableInterface) *[]sstable.Record {
	records := (*mt).Flush()
	sstRecords := make([]sstable.Record, 0, len(*records))
	for _, r := range *records {
		sstRecords = append(sstRecords, sstable.Record{
			Key:       []byte(r.Key),
			Value:     r.Value,
			KeySize:   uint64(len(r.Key)),
			ValueSize: uint64(len(r.Value)),
			Tombstone: r.Tombstone,
			Timestamp: r.Timestamp,
		})
	}
	return &sstRecords
}
