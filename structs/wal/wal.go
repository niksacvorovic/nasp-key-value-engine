package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"time"
)

// Struktura Zapisa
type Record struct {
    CRC       uint32 // CRC
    Timestamp int64  // Vreme
    Tombstone bool   // Grob
    KeySize   uint8  // Velicina kljuca
    ValueSize uint8  // Velicina vrednsoti
    Key       []byte // Kljuc
    Value     []byte // Vrednost
}

// Struktura Write-Ahead Log-a (WAL)
type WAL struct {
	file   *os.File
	buffer *bytes.Buffer
	maxSeg int // Maksimalan broj zapisa po segmentu
	count  int // Brojac zapisa u trenutnom segmentu
}

// NewWAL kreira novu instancu WAL-a
func NewWAL(filePath string, maxSeg int) (*WAL, error) {
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &WAL{
		file:   file,
		buffer: new(bytes.Buffer),
		maxSeg: maxSeg,
		count:  0,
	}, nil
}

// AppendRecord upisuje zapis u WAL
func (w *WAL) AppendRecord(tombstone bool, key, value []byte) error {
	if len(key) > 255 || len(value) > 255 {
		return errors.New("key or value exceeds maximum size")
	}

	record := Record{
		Timestamp: time.Now().Unix(),
		Tombstone: tombstone,
		KeySize:   uint8(len(key)),
		ValueSize: uint8(len(value)),
		Key:       key,
		Value:     value,
	}

	// Serijalizuj zapis u buffer
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, record.Timestamp)
	binary.Write(&buf, binary.LittleEndian, record.Tombstone)
	binary.Write(&buf, binary.LittleEndian, record.KeySize)
	binary.Write(&buf, binary.LittleEndian, record.ValueSize)
	buf.Write(record.Key)
	buf.Write(record.Value)

	record.CRC = crc32.ChecksumIEEE(buf.Bytes())
	binary.Write(w.buffer, binary.LittleEndian, record.CRC)
	w.buffer.Write(buf.Bytes())

	// Zapisi u fajl i resetuj ako je segment pun
	w.count++
	if w.count >= w.maxSeg {
		return w.flush()
	}
	return nil
}

// Close zatvara WAL fajl i flushuje sve preostale podatke na disk
func (w *WAL) Close() error {
	// Flushuj preostale podatke iz buffera
	if w.buffer.Len() > 0 {
		if err := w.flush(); err != nil {
			return err
		}
	}
	return w.file.Close()
}

// flush upisuje bufferovane podatke u WAL fajl i resetuje buffer
func (w *WAL) flush() error {
	_, err := w.file.Write(w.buffer.Bytes())
	w.buffer.Reset()
	w.count = 0
	return err
}