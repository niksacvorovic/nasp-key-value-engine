package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Struktura Zapisa
type Record struct {
	CRC       uint32   // CRC
	Timestamp [16]byte // Vreme
	Tombstone bool     // Grob
	KeySize   uint64   // Velicina kljuca
	ValueSize uint64   // Velicina vrednsoti
	Key       []byte   // Kljuc
	Value     []byte   // Vrednost
}

// Struktura Write-Ahead Log-a (WAL)
type WAL struct {
	dir     string        // Direktorijum za segmente
	file    *os.File      // Trenutni segmenti fajl
	buffer  *bytes.Buffer // Buffer
	maxSeg  int           // Maksimalno zapisa po segmentu
	count   int           // Brojac trenutnig zapisa
	segNum  int           // Trenutni broj segmenta
	oldSegs []string      // Stari segmenti koji nisu perzistirani u SSTable-u
}

// NewWAL kreira novu instancu WAL-a
func NewWAL(dirPath string, maxSeg int) (*WAL, error) {
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return nil, err
	}

	// Pronadji najveci broj segmenta
	segNum := 0
	files, _ := os.ReadDir(dirPath)
	for _, f := range files {
		if strings.HasPrefix(f.Name(), "wal_") && strings.HasSuffix(f.Name(), ".log") {
			numStr := strings.TrimPrefix(f.Name(), "wal_")
			numStr = strings.TrimSuffix(numStr, ".log")
			num, err := strconv.Atoi((numStr))
			if err == nil && num > segNum {
				segNum = num
			}
		}
	}

	// Otvori fajl
	filePath := filepath.Join(dirPath, fmt.Sprintf("wal_%04d.log", segNum+1))
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	// Vrati instancu WAL-a
	return &WAL{
		dir:     dirPath,
		file:    file,
		buffer:  new(bytes.Buffer),
		maxSeg:  maxSeg,
		count:   0,
		segNum:  segNum + 1,
		oldSegs: []string{},
	}, nil
}

// AppendRecord upisuje zapis u WAL
func (w *WAL) AppendRecord(tombstone bool, key, value []byte) error {
	record := Record{
		Tombstone: tombstone,
		KeySize:   uint64(len(key)),
		ValueSize: uint64(len(value)),
		Key:       key,
		Value:     value,
	}

	// Postavi time-stamp
	ts := fmt.Sprintf("%-16d", time.Now().UnixNano())
	copy(record.Timestamp[:], ts[:16])

	// Serijalizuj zapis (bez CRC)
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, record.Timestamp)
	binary.Write(&buf, binary.LittleEndian, record.Tombstone)
	binary.Write(&buf, binary.LittleEndian, record.KeySize)
	binary.Write(&buf, binary.LittleEndian, record.ValueSize)
	buf.Write(record.Key)
	buf.Write(record.Value)

	// Izracunaj CRC i serijalizuj cijeli zapis
	record.CRC = crc32.ChecksumIEEE(buf.Bytes())
	binary.Write(w.buffer, binary.LittleEndian, record.CRC)
	w.buffer.Write(buf.Bytes())

	w.count++
	if w.count >= w.maxSeg {
		return w.rotateSegment()
	}
	return nil
}

// rotateSegment kreira novi segmentni fajl
func (w *WAL) rotateSegment() error {
	// Flushuj trenutnio buffer
	if err := w.flush(); err != nil {
		return err
	}

	// Dodaj trenutni segment u stare segmente
	w.oldSegs = append(w.oldSegs, w.file.Name())

	// Kreiraj novi segment
	w.segNum++
	newPath := filepath.Join(w.dir, fmt.Sprintf("wal_%04d.log", w.segNum))
	newFile, err := os.OpenFile(newPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	w.file.Close()
	w.file = newFile
	w.count = 0
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

// ReadRecords cita jedan po jedan zapis iz segmenta
func (wal *WAL) ReadRecords(segmentPath string) (chan Record, chan error) {
	recordChan := make(chan Record)
	errChan := make(chan error, 1)

	go func() {
		defer close(recordChan)
		defer close(errChan)

		file, err := os.Open(segmentPath)
		if err != nil {
			errChan <- err
			return
		}
		defer file.Close()

		for {
			var record Record
			var crc uint32

			// Procitaj CRC
			if err := binary.Read(file, binary.LittleEndian, &crc); err != nil {
				if err == io.EOF {
					break
				}
				errChan <- err
				return
			}

			// Procitaj ostatak zapisa
			if err := binary.Read(file, binary.LittleEndian, &record.Timestamp); err != nil {
				errChan <- err
				return
			}
			if err := binary.Read(file, binary.LittleEndian, &record.Tombstone); err != nil {
				errChan <- err
				return
			}
			if err := binary.Read(file, binary.LittleEndian, &record.KeySize); err != nil {
				errChan <- err
				return
			}
			if err := binary.Read(file, binary.LittleEndian, &record.ValueSize); err != nil {
				errChan <- err
				return
			}

			record.Key = make([]byte, record.KeySize)
			if _, err := file.Read(record.Key); err != nil {
				errChan <- err
				return
			}

			record.Value = make([]byte, record.ValueSize)
			if _, err := file.Read(record.Value); err != nil {
				errChan <- err
				return
			}

			// Verifikuj CRC
			var buf bytes.Buffer
			binary.Write(&buf, binary.LittleEndian, record.Timestamp)
			binary.Write(&buf, binary.LittleEndian, record.Tombstone)
			binary.Write(&buf, binary.LittleEndian, record.KeySize)
			binary.Write(&buf, binary.LittleEndian, record.ValueSize)
			buf.Write(record.Key)
			buf.Write(record.Value)

			if crc32.ChecksumIEEE(buf.Bytes()) != crc {
				errChan <- errors.New("CRC mismatch - data corruption detected")
			}

			record.CRC = crc
			recordChan <- record
		}
	}()

	return recordChan, errChan
}

// MarkSegmentAsPersisted obiljezava segment perzistiranim u SSTable-u
func (w *WAL) MarkSegmentAsPersisted(segmentPath string) error {
	// Izbrisi iz starih segmenata ako postoji
	for i, seg := range w.oldSegs {
		if seg == segmentPath {
			w.oldSegs = append(w.oldSegs[:i], w.oldSegs[i+1:]...)
			return os.Remove(segmentPath)
		}
	}
	return nil
}

// flush upisuje bufferovane podatke u WAL fajl i resetuje buffer
func (w *WAL) flush() error {
	_, err := w.file.Write(w.buffer.Bytes())
	w.buffer.Reset()
	return err
}
