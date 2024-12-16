package containers

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"

	"projekat/structs/blockmanager"
	"projekat/structs/memtable"
	"projekat/structs/wal"
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
func (m *HashMapMemtable) Add(key, value string) error {
	if len(m.data) >= m.maxSize {
		fmt.Println("Memtable reached max size, writing to BlockManager...")

		// Pripremi podatke za upis
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

		err := m.blockManager.WriteBlock("file.data", 0, blockData)
		if err != nil {
			return fmt.Errorf("error writing to BlockManager: %v", err)
		}

		// Ocisti memtable poslije upisivanja
		m.data = make(map[string][]byte)
	}

	m.data[key] = []byte(value)
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
func (m *HashMapMemtable) LoadFromWAL(walPath string) error {
	return loadFromWALHelper(walPath, m)
}

// Pomocna funkcija za ucitavanje podataka iz WAL-a (zajednicka logika)
func loadFromWALHelper(walPath string, memtable memtable.MemtableInterface) error {
	file, err := os.Open(walPath)
	if err != nil {
		return fmt.Errorf("ne mogu otvoriti WAL fajl: %v", err)
	}
	defer file.Close()

	for {
		record, err := readRecord(file)
		if err != nil {
			if err.Error() == "EOF" { // Kraj fajla
				break
			}
			return fmt.Errorf("greska pri citanju zapisa iz WAL-a: %v", err)
		}

		if record.Tombstone {
			memtable.Delete(string(record.Key))
		} else {
			memtable.Add(string(record.Key), string(record.Value))
		}
	}
	return nil
}

// readRecord cita zapis iz WAL fajla
func readRecord(file *os.File) (*wal.Record, error) {
	var crc uint32
	var timestamp int64
	var tombstone bool
	var keySize, valueSize uint8

	// Citanje zaglavlja zapisa
	err := binary.Read(file, binary.LittleEndian, &crc)
	if err != nil {
		return nil, err
	}
	err = binary.Read(file, binary.LittleEndian, &timestamp)
	if err != nil {
		return nil, err
	}
	err = binary.Read(file, binary.LittleEndian, &tombstone)
	if err != nil {
		return nil, err
	}
	err = binary.Read(file, binary.LittleEndian, &keySize)
	if err != nil {
		return nil, err
	}
	err = binary.Read(file, binary.LittleEndian, &valueSize)
	if err != nil {
		return nil, err
	}

	// Citanje kljuca i vrednosti
	key := make([]byte, keySize)
	_, err = file.Read(key)
	if err != nil {
		return nil, err
	}
	value := make([]byte, valueSize)
	_, err = file.Read(value)
	if err != nil {
		return nil, err
	}

	// Verifikacija CRC
	data := append([]byte{}, memtable.TimestampToBytes(timestamp)...)
	data = append(data, memtable.BoolToByte(tombstone))
	data = append(data, keySize, valueSize)
	data = append(data, key...)
	data = append(data, value...)
	calculatedCRC := crc32.ChecksumIEEE(data)
	if calculatedCRC != crc {
		return nil, fmt.Errorf("ne odgovara CRC")
	}

	return &wal.Record{
		CRC:       crc,
		Timestamp: timestamp,
		Tombstone: tombstone,
		KeySize:   keySize,
		ValueSize: valueSize,
		Key:       key,
		Value:     value,
	}, nil
}
