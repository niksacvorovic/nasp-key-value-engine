package memtable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"

	"projekat/structs/wal"
)

// Jedan zapis ključ - vrednost
type Record struct {
	Key   string
	Value []byte
}

// MemtableInterface definise zajednicki interfejs za sve implementacije Memtable-a
type MemtableInterface interface {
	Add(key string, value []byte) error
	Delete(key string) error
	Get(key string) ([]byte, bool)
	// PrintData()
	LoadFromWAL(file *os.File, offset int64) (int64, error)
	Flush() *[]Record
	IsFull() bool
}

// Greška ukoliko je Memtable popunjen
var MemtableFull error = errors.New("memtable full")

// Utility funkcije za konverziju tipova
func TimestampToBytes(ts int64) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, ts)
	return buf.Bytes()
}

func BoolToByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

// Pomocna funkcija za ucitavanje podataka iz WAL-a (zajednicka logika)
func LoadFromWALHelper(file *os.File, memtable MemtableInterface, offset int64) (int64, error) {
	file.Seek(offset, 0)

	for {
		record, err := readRecord(file)
		if err != nil {
			if err.Error() == "EOF" { // Kraj fajla
				break
			}
			return 0, fmt.Errorf("greska pri citanju zapisa iz WAL-a: %v", err)
		}

		if record.Tombstone {
			memtable.Delete(string(record.Key))
		} else {
			err = memtable.Add(string(record.Key), record.Value)
			if err == MemtableFull {
				offset, _ = file.Seek(0, 1)
				return offset, err
			}
		}
	}
	return 0, nil
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
	data := append([]byte{}, TimestampToBytes(timestamp)...)
	data = append(data, BoolToByte(tombstone))
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

// Potencijalno možemo premjestiti još generalnih funckija u interfejs da nema redundantnog koda
