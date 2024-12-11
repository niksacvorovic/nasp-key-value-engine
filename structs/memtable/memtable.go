package memtable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"projekat/structs/containers"
	"projekat/structs/wal"
)

type Memtable struct {
	ContainerInstance *containers.Container
}

// func Add(key, value string) error
// func Delete(key string) error
// func Get(key string) (string, bool)
// func PrintData()
// func LoadFromWAL(walPath string) error

func (m *Memtable) Add(key string, value []byte) {
	(*m.ContainerInstance).WriteElement(key, value)
}

// LoadFromWAL ucitava podatke iz WAL fajla u HashMapMemtable
func (m *Memtable) LoadFromWAL(walPath string) error {
	return loadFromWALHelper(walPath, m)
}

// Pomocna funkcija za ucitavanje podataka iz WAL-a (zajednicka logika)
func loadFromWALHelper(walPath string, memtable *Memtable) error {
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
		memtable.Add(string(record.Key), record.Value)
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
