package containers

import (
	"fmt"
	"math"
	"sort"

	"projekat/structs/memtable"
)

// HashMapMemtable implementira MemtableInterface koristeci mapu
type HashMapMemtable struct {
	data      map[string]memtable.Record
	watermark uint32
	maxSize   int
}

// NewHashMapMemtable kreira novu instancu HashMapMemtable-a
func NewHashMapMemtable(maxSize int) *HashMapMemtable {
	return &HashMapMemtable{
		data:      make(map[string]memtable.Record),
		watermark: math.MaxUint32,
		maxSize:   maxSize,
	}
}

// Add dodaje par kljuc-vrednost u HashMapMemtable
func (m *HashMapMemtable) Add(ts [16]byte, tombstone bool, key string, value []byte) error {
	m.data[key] = memtable.Record{Timestamp: ts, Tombstone: tombstone, Key: key, Value: value}
	return nil
}

// Vraca true ako je memtable pun, a u suprotnom false
func (m *HashMapMemtable) IsFull() bool {
	return len(m.data) >= m.maxSize+1
}

// Delete uklanja par kljuc-vrednost iz HashMapMemtable-a
func (m *HashMapMemtable) Delete(key string) error {
	if _, exists := m.data[key]; !exists {
		return fmt.Errorf("kljuc %s ne postoji u Memtable-u", key)
	}
	delete(m.data, key)
	return nil
}

// Get dohvata vrednost prema kljucu iz HashMapMemtable-a
func (m *HashMapMemtable) Get(key string) ([]byte, bool) {
	record, exists := m.data[key]
	return record.Value, exists
}

// PrintData ispisuje sve podatke u HashMapMemtable-u
func (m *HashMapMemtable) PrintData() {
	fmt.Println("Sadrzaj HashMapMemtable-a:")
	for key, value := range m.data {
		fmt.Printf("Kljuc: %s, Vrednost: %s\n", key, value)
	}
}

// LoadFromWAL ucitava podatke iz WAL fajla u HashMapMemtable
// func (m *HashMapMemtable) LoadFromWAL(file *os.File, offset int64) (int64, error) {
// return memtable.LoadFromWALHelper(file, m, offset)
// }

// SerializeToSSTable serijalizuje podatke iz Memtable-a u SSTable
func (m *HashMapMemtable) Flush() *[]memtable.Record {
	// Sortiramo ključeve
	sortedKeys := make([]string, 0, len(m.data))
	for key := range m.data {
		sortedKeys = append(sortedKeys, key)
	}
	sort.Strings(sortedKeys)
	// Dodajemo vrednosti u niz
	records := make([]memtable.Record, 0, len(m.data))
	for i := range sortedKeys {
		records = append(records, m.data[sortedKeys[i]])
	}
	// Resetujemo Memtable
	m.data = make(map[string]memtable.Record)
	return &records
}

func (m *HashMapMemtable) SetWatermark(index uint32) {
	m.watermark = min(m.watermark, index)
}

func (m *HashMapMemtable) GetWatermark() uint32 {
	return m.watermark
}
