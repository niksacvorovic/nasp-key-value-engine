package memtable

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"hash/crc32"
	"bytes"

	"projekat/structs/wal"
)

// HashMapMemtable implementira MemtableInterface koristeci mapu
type HashMapMemtable struct {
	data    map[string]string
	maxSize int
	mu      sync.RWMutex
}

// NewHashMapMemtable kreira novu instancu HashMapMemtable-a
func NewHashMapMemtable(maxSize int) *HashMapMemtable {
	return &HashMapMemtable{
		data:    make(map[string]string),
		maxSize: maxSize,
	}
}

// Add dodaje par kljuc-vrednost u HashMapMemtable
func (m *HashMapMemtable) Add(key, value string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.data) >= m.maxSize {
		return fmt.Errorf("Memtable je pun")
	}
	m.data[key] = value
	return nil
}

// Delete uklanja par kljuc-vrednost iz HashMapMemtable-a
func (m *HashMapMemtable) Delete(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.data[key]; !exists {
		return fmt.Errorf("Kljuc %s ne postoji u Memtable-u", key)
	}
	delete(m.data, key)
	return nil
}

// Get dohvata vrednost prema kljucu iz HashMapMemtable-a
func (m *HashMapMemtable) Get(key string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	value, exists := m.data[key]
	return value, exists
}

// PrintData ispisuje sve podatke u HashMapMemtable-u
func (m *HashMapMemtable) PrintData() {
	m.mu.RLock()
	defer m.mu.RUnlock()

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
func loadFromWALHelper(walPath string, memtable MemtableInterface) error {
	file, err := os.Open(walPath)
	if err != nil {
		return fmt.Errorf("Ne mogu otvoriti WAL fajl: %v", err)
	}
	defer file.Close()

	for {
		record, err := readRecord(file)
		if err != nil {
			if err.Error() == "EOF" { // Kraj fajla
				break
			}
			return fmt.Errorf("Greska pri citanju zapisa iz WAL-a: %v", err)
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
	data := append([]byte{}, timestampToBytes(timestamp)...)
	data = append(data, boolToByte(tombstone))
	data = append(data, keySize, valueSize)
	data = append(data, key...)
	data = append(data, value...)
	calculatedCRC := crc32.ChecksumIEEE(data)
	if calculatedCRC != crc {
		return nil, fmt.Errorf("CRC ne odgovara")
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

// Pomocne funkcije
func timestampToBytes(ts int64) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, ts)
	return buf.Bytes()
}

func boolToByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}