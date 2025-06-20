package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"projekat/structs/blockmanager"
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
	bm                      *blockmanager.BlockManager // Blockmanager
	Dir                     string                     // Direktorijum za segmente
	file                    *os.File                   // Trenutni segmenti fajl
	buffer                  *bytes.Buffer              // Buffer
	walMaxRecordsPerSegment int                        // Maksimalno zapisa po segmentu
	walBlokcsPerSegment     int                        // Broj blokova po segmentu
	recordCount             int                        // Brojac zapisa u segmentu
	segNum                  int                        // Trenutni broj segmenta
	oldSegs                 []string                   // Stari segmenti koji nisu perzistirani u SSTable-u
}

// NewWAL kreira novu instancu WAL-a
func NewWAL(dirPath string, walMaxRecordsPerSegment int, walBlokcsPerSegment int, blockSize int, blockCacheSize int) (*WAL, error) {
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

	// Otvori fajl sledeceg segmenta
	filePath := filepath.Join(dirPath, fmt.Sprintf("wal_%04d.log", segNum+1))
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	// Vrati instancu WAL-a
	return &WAL{
		bm:                      blockmanager.NewBlockManager(blockSize, blockCacheSize),
		Dir:                     dirPath,
		file:                    file,
		buffer:                  new(bytes.Buffer),
		walMaxRecordsPerSegment: walMaxRecordsPerSegment,
		walBlokcsPerSegment:     walBlokcsPerSegment,
		recordCount:             0,
		segNum:                  segNum + 1, // Novi broj segmenta
		oldSegs:                 []string{},
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

	// Zapisi zapis putem BlockManager-a
	err := w.bm.WriteBlock(w.file.Name(), w.buffer.Bytes())
	if err != nil {
		return err
	}

	// Resetuj buffer
	w.buffer.Reset()

	// Ako smo stigli do maksimalnog broja zapisa ili blokova prelazimo na sledeci segment
	w.recordCount++
	if w.recordCount >= w.walMaxRecordsPerSegment || w.bm.Block_idx >= w.walBlokcsPerSegment {
		return w.rotateSegment()
	}

	return nil
}

// rotateSegment kreira novi segmentni fajl
func (w *WAL) rotateSegment() error {
	// Dodaj trenutni segment u stare segmente
	w.oldSegs = append(w.oldSegs, w.file.Name())

	// Kreiraj novi segment
	w.segNum++
	newPath := filepath.Join(w.Dir, fmt.Sprintf("wal_%04d.log", w.segNum))
	newFile, err := os.OpenFile(newPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	// Resetuj sve vrijednosti
	w.file.Close()
	w.file = newFile
	w.recordCount = 0
	w.bm.Block_idx = 0
	return nil
}

// Close zatvara WAL fajl
func (w *WAL) Close() error {
	return w.file.Close()
}

// ReadRecords cita jedan po jedan zapis iz segmenta
func (w *WAL) ReadRecords(segmentPath string) ([]Record, error) {
	records := make([]Record, 0, w.walBlokcsPerSegment)

	for idx := 0; idx < w.walBlokcsPerSegment; idx++ {
		// Procitaj zapi
		block, err := w.bm.ReadBlock(segmentPath, idx)
		if err != nil {
			return nil, err
		}

		// Ako je zapis prazan predji na sledeci
		if isBlockEmpty(block) {
			continue
		}

		// Parsiraj zapis
		rec, err := parseRecord(block)
		if err != nil {
			return nil, err
		}

		records = append(records, rec)
	}

	return records, nil
}

// paraseRecord parsiraj jedan zapis tipa []byta u tip Record
func parseRecord(block []byte) (Record, error) {
	var rec Record

	if len(block) < 37 {
		return rec, errors.New("block too small for record header")
	}

	// Prvo procitaj CRC (prvih 4 bajta)
	rec.CRC = binary.LittleEndian.Uint32(block[0:4])

	// Racunaj CRC nad ostalim bajtovima (od 4 pa do kraja podataka)
	// Racunaj ukupnu duzinu na osnovu KeySize i ValueSize
	keySize := binary.LittleEndian.Uint64(block[21:29])
	valueSize := binary.LittleEndian.Uint64(block[29:37])
	expectedLen := 37 + int(keySize) + int(valueSize)

	if len(block) < expectedLen {
		return rec, errors.New("block too small for key and value")
	}

	// Citaj sve osim CRC za validaciju
	dataForCRC := block[4:expectedLen]

	// Izracunaj CRC na osnovu tih podataka i provjeri da li je ispravan
	calculatedCRC := crc32.ChecksumIEEE(dataForCRC)
	if calculatedCRC != rec.CRC {
		return rec, errors.New("CRC mismatch - data corruption detected")
	}

	// Ako je CRC OK, popuni ostala polja
	copy(rec.Timestamp[:], block[4:20])
	rec.Tombstone = block[20] != 0
	rec.KeySize = keySize
	rec.ValueSize = valueSize

	rec.Key = make([]byte, rec.KeySize)
	copy(rec.Key, block[37:37+rec.KeySize])

	rec.Value = make([]byte, rec.ValueSize)
	copy(rec.Value, block[37+rec.KeySize:expectedLen])

	return rec, nil
}

// MarkSegmentAsPersisted obiljezava segment perzistiranim u SSTable-u
// TODO vjerovatno izmjeniti kad dodje do brisanja
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

// isBlockEmpty provjerava da li je blok prazan (ako je prvih 37 bajtova 0)
func isBlockEmpty(block []byte) bool {
	if len(block) < 37 {
		return false
	}
	for i := 0; i < 37; i++ {
		if block[i] != 0 {
			return false
		}
	}
	return true
}
