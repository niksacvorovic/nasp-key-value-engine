package containers

import (
	"fmt"
	"os"
	"sort"

	"projekat/structs/blockmanager"
	"projekat/structs/memtable"
)

// HashMapMemtable implementira MemtableInterface koristeci mapu
type HashMapMemtable struct {
	data         map[string][]byte
	maxSize      int
	blockManager *blockmanager.BlockManager
}

// NewHashMapMemtable kreira novu instancu HashMapMemtable-a
func NewHashMapMemtable(maxSize int, blockManager *blockmanager.BlockManager) *HashMapMemtable {
	return &HashMapMemtable{
		data:         make(map[string][]byte),
		maxSize:      maxSize,
		blockManager: blockManager,
	}
}

// Add dodaje par kljuc-vrednost u HashMapMemtable
func (m *HashMapMemtable) Add(key string, value []byte) error {
	m.data[key] = value

	return nil
}

func (m *HashMapMemtable) IsFull() (bool) {
	if len(m.data) >= m.maxSize + 1 {
		return true
	}
	return false
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
	value, exists := m.data[key]
	return value, exists
}

// PrintData ispisuje sve podatke u HashMapMemtable-u
func (m *HashMapMemtable) PrintData() {
	fmt.Println("Sadrzaj HashMapMemtable-a:")
	for key, value := range m.data {
		fmt.Printf("Kljuc: %s, Vrednost: %s\n", key, value)
	}
}

// LoadFromWAL ucitava podatke iz WAL fajla u HashMapMemtable
func (m *HashMapMemtable) LoadFromWAL(file *os.File, offset int64) (int64, error) {
	return memtable.LoadFromWALHelper(file, m, offset)
}

func (m *HashMapMemtable) SerializeToSSTable(filename string, BlockSize int) error {
	// Sortiramo ključeve
	keys := make([]string, 0, len(m.data))
	for key := range m.data {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// Serijalizacija podataka u SSTable format
	blockData := make([]byte, 0, m.maxSize*10)
	for _, key := range keys {
		value := m.data[key]

		// Dužina ključa i vrednosti
		keyLen := len(key)
		valueLen := len(value)

		// Serijalizacija u binarni format: [keyLen][key][valueLen][value]
		blockData = append(blockData, byte(keyLen))
		blockData = append(blockData, []byte(key)...)
		blockData = append(blockData, byte(valueLen))
		blockData = append(blockData, value...)
	}

	padding := BlockSize - (len(blockData) % BlockSize)
	if padding < BlockSize {
		blockData = append(blockData, make([]byte, padding)...)
	}

	// Pisanje podataka u SSTable fajl koristeći BlockManager
	err := m.blockManager.WriteBlock(filename, blockData)
	if err != nil {
		return fmt.Errorf("greška pri pisanju u BlockManager: %v", err)
	}

	// Resetujemo Memtable
	m.data = make(map[string][]byte)

	fmt.Printf("Podaci uspešno serijalizovani u SSTable fajl: %s\n", filename)
	return nil
}
