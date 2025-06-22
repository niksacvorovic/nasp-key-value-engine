package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"projekat/structs/blockmanager"
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
	segments                map[int]string             // Mapa svih segmenata WAL
	buffer                  []byte                     // Buffer
	blockSize               int                        // Veličina jednog bloka
	walMaxRecordsPerSegment int                        // Maksimalno zapisa po segmentu
	walBlocksPerSegment     int                        // Broj blokova po segmentu
	recordCount             int                        // Brojac zapisa u segmentu
	segNum                  int                        // Trenutni broj segmenta
}

func (r *Record) CalculateSize() int {
	return 4 + 16 + 1 + 8 + 8 + int(r.KeySize) + int(r.ValueSize)
}

func (r *Record) RecordToBytes() []byte {
	bytes := make([]byte, 0)
	bytes = append(bytes, r.Timestamp[:]...)
	binary.Append(bytes, binary.LittleEndian, r.Tombstone)
	binary.Append(bytes, binary.LittleEndian, r.KeySize)
	binary.Append(bytes, binary.LittleEndian, r.ValueSize)
	bytes = append(bytes, r.Key...)
	bytes = append(bytes, r.Value...)
	r.CRC = crc32.ChecksumIEEE(bytes)
	bytes = append(binary.LittleEndian.AppendUint32([]byte{}, r.CRC), bytes...)
	return bytes
}

// NewWAL kreira novu instancu WAL-a
func NewWAL(dirPath string, walMaxRecordsPerSegment int, walBlocksPerSegment int,
	blockSize int, blockCacheSize int) (*WAL, error) {
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return nil, err
	}

	newBM := blockmanager.NewBlockManager(blockSize, blockCacheSize)

	// Pronadji najveci broj segmenta
	orderedFiles := make(map[int]string)
	segNum := 0
	var order int32
	contents, _ := os.ReadDir(dirPath)
	for _, f := range contents {
		if !f.IsDir() {
			block, err := newBM.ReadBlock(filepath.Join(dirPath, f.Name()), 0)
			if err == nil {
				segNum = int(binary.LittleEndian.Uint32(block[3:7]))
				if string(block[:3]) == "WAL" {
					orderedFiles[int(order)] = f.Name()
					if int(order) > segNum {
						segNum = int(order)
					}
				}
			}
		}
	}
	// Određivanje indeksa poslednjeg bloka u poslednjem segmentu fajla
	file, _ := os.OpenFile(filepath.Join(dirPath, orderedFiles[segNum]), os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	fileinfo, _ := file.Stat()
	newBM.Block_idx = int(fileinfo.Size())/blockSize - 1

	// Vrati instancu WAL-a
	return &WAL{
		bm:                      newBM,
		Dir:                     dirPath,
		segments:                orderedFiles,
		buffer:                  make([]byte, 0, blockSize),
		walMaxRecordsPerSegment: walMaxRecordsPerSegment,
		walBlocksPerSegment:     walBlocksPerSegment,
		recordCount:             0,
		blockSize:               blockSize,
		segNum:                  segNum + 1, // Novi broj segmenta
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
	ts := fmt.Sprintf("%-16d", time.Now().Unix())
	copy(record.Timestamp[:], ts[:16])

	blockSpace := w.blockSize - len(w.buffer)
	recordBytes := record.RecordToBytes()
	if blockSpace >= len(recordBytes) && w.recordCount > w.walMaxRecordsPerSegment {
		w.buffer = append(w.buffer, recordBytes...)
		w.recordCount++
	} else if blockSpace >= len(recordBytes) && w.recordCount == w.walMaxRecordsPerSegment {
		w.bm.WriteBlock(filepath.Join(w.Dir, w.segments[w.segNum]), w.buffer)
		w.rotateSegment()
	} else if blockSpace < len(recordBytes) {
		remaining := 0
		for remaining < len(recordBytes) {
			if blockSpace > len(recordBytes)-remaining {
				w.buffer = append(w.buffer, recordBytes[remaining:]...)
			} else {
				w.buffer = append(w.buffer, recordBytes[remaining:remaining+blockSpace]...)
			}
			remaining += blockSpace
			if len(w.buffer) == w.blockSize {
				w.bm.WriteBlock(filepath.Join(w.Dir, w.segments[w.segNum]), w.buffer)
				w.bm.Block_idx++
				blockSpace = w.blockSize
				if w.bm.Block_idx == w.walBlocksPerSegment {
					w.rotateSegment()
					blockSpace = w.blockSize - 7 // header je sedam bajtova
				}
			}
		}
	}

	// Ako smo stigli do maksimalnog broja zapisa ili blokova prelazimo na sledeci segment
	w.recordCount++
	if w.recordCount >= w.walMaxRecordsPerSegment || w.bm.Block_idx >= w.walBlocksPerSegment {
		return w.rotateSegment()
	}
	return nil
}

// rotateSegment kreira novi segmentni fajl
func (w *WAL) rotateSegment() error {
	// Kreiraj novi segment
	w.segNum++
	newPath := filepath.Join(w.Dir, fmt.Sprintf("wal_%04d.log", w.segNum))
	newFile, err := os.OpenFile(newPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	// Dodaj novi segment u spisak segmenata
	w.segments[w.segNum] = newFile.Name()

	// Resetuj sve vrijednosti
	w.recordCount = 0
	w.buffer = make([]byte, 0, w.blockSize)
	w.buffer = append(w.buffer, []byte("WAL")...)
	binary.LittleEndian.AppendUint32(w.buffer, uint32(w.segNum))
	w.bm.Block_idx = 0
	return nil
}

// ReadRecords čita sve segmente iz WAL
func (w *WAL) ReadRecords() ([]Record, error) {
	records := make([]Record, 0)
	cutoff := make([]byte, 0)
	newrecs := make([]Record, 0)
	for i := 0; i < w.segNum; i++ {
		for j := 0; j < w.walBlocksPerSegment; i++ {
			block, err := w.bm.ReadBlock(filepath.Join(w.Dir, w.segments[i]), j)
			if err == io.EOF {
				return records, nil
			}
			if i == 0 {
				chunk := append(cutoff, block[7:]...)
				newrecs, cutoff, _ = parseRecord(chunk)
			} else {
				chunk := append(cutoff, block...)
				newrecs, cutoff, _ = parseRecord(chunk)
			}
			records = append(records, newrecs...)
		}
	}
	return records, nil
}

// paraseRecord parsiraj jedan zapis tipa []byta u tip Record
func parseRecord(block []byte) ([]Record, []byte, error) {
	var rec Record
	var recs []Record
	seek := 0
	for {
		// Procitaj zaglavlje zapisa
		recordBytes := make([]byte, 0)
		if len(block)-seek < 21 {
			return recs, block[seek:], nil
		}
		recordBytes = append(recordBytes, block[seek:seek+21]...)
		seek += 21
		// Procitaj KeySize
		if len(block)-seek < 8 {
			return recs, block[seek-21:], nil
		}
		rec.KeySize = binary.LittleEndian.Uint64(block[seek : seek+8])
		recordBytes = append(recordBytes, block[seek:seek+8]...)
		seek += 8
		// Procitaj ValueSize
		if len(block)-seek < 8 {
			return recs, block[seek-29:], nil
		}
		rec.ValueSize = binary.LittleEndian.Uint64(block[seek : seek+8])
		recordBytes = append(recordBytes, block[seek:seek+8]...)
		seek += 8
		// Procitaj Key i Value
		if len(block)-seek < int(rec.KeySize)+int(rec.ValueSize) {
			return recs, block[seek-37:], nil
		}
		recordBytes = append(recordBytes, block[seek:seek+int(rec.KeySize)+int(rec.ValueSize)]...)
		seek += int(rec.KeySize) + int(rec.ValueSize)
		crc := crc32.ChecksumIEEE(recordBytes[4:])
		if crc == binary.LittleEndian.Uint32(recordBytes[:4]) {
			rec.CRC = binary.LittleEndian.Uint32(recordBytes[:4])
			copy(rec.Timestamp[:], recordBytes[4:20])
			if recordBytes[20] == 0 {
				rec.Tombstone = true
			} else {
				rec.Tombstone = false
			}
			rec.Key = recordBytes[37 : 37+rec.KeySize]
			rec.Value = recordBytes[37+rec.KeySize : 37+rec.KeySize+rec.ValueSize]
			recs = append(recs, rec)
		}
	}
}

// MarkSegmentAsPersisted obiljezava segment perzistiranim u SSTable-u
func (w *WAL) MarkSegmentAsPersisted(segmentPath string) error {
	// Izbrisi iz starih segmenata ako postoji
	for i := 0; i < w.segNum; i++ {
		if filepath.Join(w.Dir, w.segments[i]) == segmentPath {
			delete(w.segments, i)
			return os.Remove(segmentPath)
		}
	}
	return nil
}
