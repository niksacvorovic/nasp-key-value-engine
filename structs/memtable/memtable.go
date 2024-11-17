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

// Memtable struktura
type Memtable struct {
	data    map[string]string // Kljuc/vrednost
	maxSize int               // Maksimalni broj elemenata
	mu      sync.RWMutex      // Mutex za sinhronizaciju
}

// NewMemtable kreira novu instancu Memtable-a.
func NewMemtable(maxSize int) *Memtable {
	return &Memtable{
		data:    make(map[string]string),
		maxSize: maxSize,
	}
}

// Add dodaje novi element u Memtable.
func (m *Memtable) Add(key, value string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.data) >= m.maxSize {
		// ----------------------------------------------------------------------------------------------------------
		// Kada se dostigne maksimalna velicina Memtable-a podaci se upisuju na disk u strukturu SSTable
		// ----------------------------------------------------------------------------------------------------------
		return fmt.Errorf("Memtable je pun")
	}
	m.data[key] = value
	return nil
}

// Delete logicki brise element iz Memtable-a.
func (m *Memtable) Delete(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.data[key]; !exists {
		return fmt.Errorf("Kljuc %s ne postoji u Memtable-u", key)
	}
	delete(m.data, key)
	return nil
}

// Get vraca vrednost za odredjeni kljuc.
func (m *Memtable) Get(key string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	value, exists := m.data[key]
	return value, exists
}

// LoadFromWAL popunjava Memtable podacima iz WAL-a.
func (m *Memtable) LoadFromWAL(walPath string) error {
	file, err := os.Open(walPath)
	if err != nil {
		return fmt.Errorf("Ne mogu otvoriti WAL fajl: %v", err)
	}
	defer file.Close()

	for {
		// Parsiranje jednog zapisa iz WAL-a
		record, err := readRecord(file)
		if err != nil {
			if err.Error() == "EOF" { // Kraj fajla
				break
			}
			return fmt.Errorf("Greska pri citanju zapisa iz WAL-a: %v", err)
		}

		// Ako je zapis oznacen kao tombstone, brisemo kljuc iz Memtable-a
		if record.Tombstone {
			m.Delete(string(record.Key))
		} else {
			// Dodajemo kljuc i vrednost u Memtable
			m.Add(string(record.Key), string(record.Value))
		}
	}
	return nil
}

// readRecord cita jedan zapis iz WAL fajla.
func readRecord(file *os.File) (*wal.Record, error) {
	var crc uint32
	var timestamp int64
	var tombstone bool
	var keySize, valueSize uint8

	// Citaj zaglavlje zapisa
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

	// Citaj kljuc i vrednost
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

	// Verifikuj CRC (opciono, za dodatnu bezbednost)
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

// PrintData ispisuje sve podatke iz Memtable-a
func (m *Memtable) PrintData() {
	m.mu.RLock()
	defer m.mu.RUnlock()

	fmt.Println("Sadrzaj Memtable-a:")
	for key, value := range m.data {
		fmt.Printf("Kljuc: %s, Vrednost: %s\n", key, value)
	}
}

// Pomocne funkcije za konverzije
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