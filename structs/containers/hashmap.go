package containers

import (
	"fmt"
	"os"

	"projekat/structs/blockmanager"
	"projekat/structs/memtable"
)

// HashMapMemtable implementira MemtableInterface koristeci mapu
type HashMapMemtable struct {
	data         map[string][]byte
	maxSize      int
	blockManager *blockmanager.BlockManager
	block_idx    int
}

// NewHashMapMemtable kreira novu instancu HashMapMemtable-a
func NewHashMapMemtable(maxSize int, blockManager *blockmanager.BlockManager) *HashMapMemtable {
	return &HashMapMemtable{
		data:         make(map[string][]byte),
		maxSize:      maxSize,
		blockManager: blockManager,
		block_idx:    0,
	}
}

// Add dodaje par kljuc-vrednost u HashMapMemtable
func (m *HashMapMemtable) Add(key, value string) error {
	m.data[key] = []byte(value)

	if len(m.data) >= m.maxSize {
		return memtable.MemtableFull
	}

	return nil
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
func (m *HashMapMemtable) Get(key string) (string, bool) {
	value, exists := m.data[key]
	return string(value), exists
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

func (m *HashMapMemtable) Serialize() error {
	blockData := make([]byte, 0, m.maxSize*10)
	for k, v := range m.data {
		keyLen := len(k)
		valueLen := len(v)
		blockData = append(blockData, byte(keyLen))
		blockData = append(blockData, []byte(k)...)
		blockData = append(blockData, byte(valueLen))
		blockData = append(blockData, v...)
	}
	blockData = append(blockData, make([]byte, 4096-len(blockData))...)
	err := m.blockManager.WriteBlock("file.data", m.block_idx, blockData)
	if err != nil {
		return fmt.Errorf("error writing to BlockManager: %v", err)
	}
	m.block_idx += 1

	return nil
}
