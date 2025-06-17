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
			if err == ErrMemtableFull {
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
	var record wal.Record

	// Read CRC first
	err := binary.Read(file, binary.LittleEndian, &crc)
	if err != nil {
		return nil, err
	}

	// Read the rest of the record
	err = binary.Read(file, binary.LittleEndian, &record.Timestamp)
	if err != nil {
		return nil, err
	}
	err = binary.Read(file, binary.LittleEndian, &record.Tombstone)
	if err != nil {
		return nil, err
	}
	err = binary.Read(file, binary.LittleEndian, &record.KeySize)
	if err != nil {
		return nil, err
	}
	err = binary.Read(file, binary.LittleEndian, &record.ValueSize)
	if err != nil {
		return nil, err
	}

	// Read key and value
	record.Key = make([]byte, record.KeySize)
	_, err = file.Read(record.Key)
	if err != nil {
		return nil, err
	}

	record.Value = make([]byte, record.ValueSize)
	_, err = file.Read(record.Value)
	if err != nil {
		return nil, err
	}

	// Verify CRC
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, record.Timestamp)
	binary.Write(&buf, binary.LittleEndian, record.Tombstone)
	binary.Write(&buf, binary.LittleEndian, record.KeySize)
	binary.Write(&buf, binary.LittleEndian, record.ValueSize)
	buf.Write(record.Key)
	buf.Write(record.Value)

	calculatedCRC := crc32.ChecksumIEEE(buf.Bytes())
	if calculatedCRC != crc {
		return nil, fmt.Errorf("CRC mismatch - data corruption detected")
	}

	record.CRC = crc
	return &record, nil
}
