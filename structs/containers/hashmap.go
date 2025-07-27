package containers

import (
	"sort"

	"projekat/structs/cursor"
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
		watermark: 0,
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
	return len(m.data) == m.maxSize
}

// Delete uklanja par kljuc-vrednost iz HashMapMemtable-a
func (m *HashMapMemtable) Delete(key string) bool {
	if record, exists := m.data[key]; !exists {
		return false
	} else {
		record.Tombstone = true
		m.data[key] = record
		return true
	}
}

// Get dohvata vrednost prema kljucu iz HashMapMemtable-a
func (m *HashMapMemtable) Get(key string) ([]byte, bool, bool) {
	record, exists := m.data[key]
	if exists {
		return record.Value, record.Tombstone, exists
	}
	return []byte{}, false, exists
}

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
	for _, key := range sortedKeys {
		records = append(records, m.data[key])
	}
	// Resetujemo Memtable
	m.data = make(map[string]memtable.Record)
	return &records
}

func (m *HashMapMemtable) SetWatermark(index uint32) {
	m.watermark = max(m.watermark, index)
}

func (m *HashMapMemtable) GetWatermark() uint32 {
	return m.watermark
}

// --------------------------------------------------------------------------------------------------------------------------
// HashMap cursor
// --------------------------------------------------------------------------------------------------------------------------

// Hashmap cursor struktura
type HashMapCursor struct {
	memtable *HashMapMemtable
	keys     []string
	current  int
}

// NewCursor vraca instancu cursora za HashMap memtabelu
func (m *HashMapMemtable) NewCursor() cursor.Cursor {

	// Pokupi i sortiraj sve kljuceve unutar memtabele
	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	return &HashMapCursor{
		memtable: m,
		keys:     keys,
		current:  -1,
	}
}

// Seek pronalazi prvi zapis unutar memtabele koji je >= minKey
func (c *HashMapCursor) Seek(minKey string) bool {
	if len(c.keys) == 0 {
		return false
	}

	// Pronadji prvi kljuc koji je >= minKey
	c.current = sort.SearchStrings(c.keys, minKey) - 1
	return c.Next()
}

// Funkcija za prelazak na sledeci zapis
func (c *HashMapCursor) Next() bool {
	if len(c.keys) == 0 {
		return false
	}

	c.current++
	if c.current >= len(c.keys) {
		return false
	}

	// Preskoci ako je trenutni kljuc izvan opsega
	return true
}

// Getter za kljuc
func (c *HashMapCursor) Key() string {
	if c.current < 0 || c.current >= len(c.keys) {
		return ""
	}
	return c.keys[c.current]
}

// Getter za vrijednost
func (c *HashMapCursor) Value() []byte {
	if c.current < 0 || c.current >= len(c.keys) {
		return nil
	}
	return c.memtable.data[c.keys[c.current]].Value
}

// Getter za timestamp
func (c *HashMapCursor) Timestamp() [16]byte {
	if c.current < 0 || c.current >= len(c.keys) {
		return [16]byte{}
	}
	return c.memtable.data[c.keys[c.current]].Timestamp
}

// Getter za tombstone
func (c *HashMapCursor) Tombstone() bool {
	if c.current < 0 || c.current >= len(c.keys) {
		return false
	}
	return c.memtable.data[c.keys[c.current]].Tombstone
}

// Funckija za reset cursora
func (c *HashMapCursor) Close() {
	c.memtable = nil
	c.keys = nil
	c.current = -1
}
