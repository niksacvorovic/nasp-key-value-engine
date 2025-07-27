package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
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
	Type      byte     // Tip zapisa (ceo, prvi, srednji, poslednji)
	KeySize   uint64   // Velicina kljuca
	ValueSize uint64   // Velicina vrednsoti
	Key       []byte   // Kljuc
	Value     []byte   // Vrednost
}

// Struktura Write-Ahead Log-a (WAL)
type WAL struct {
	bm                  *blockmanager.BlockManager // Blockmanager
	Dir                 string                     // Direktorijum za segmente
	segments            map[uint32]string          // Mapa svih segmenata WAL
	sizes               map[uint32]int             // Mapa veličina segmenata u blokovima
	buffer              []byte                     // Buffer
	blockSize           int                        // Veličina jednog bloka
	walBlocksPerSegment int                        // Broj blokova po segmentu
	LastSeg             uint32                     // Indeks poslednjeg segmenta
	FirstSeg            uint32                     // Redni broj prvog segmenta
}

func (r *Record) CalculateSize() int {
	return 4 + 16 + 1 + 1 + 8 + 8 + int(r.KeySize) + int(r.ValueSize)
}

func (r *Record) RecordToBytes() []byte {
	bytes := make([]byte, 0)
	bytes = append(bytes, r.Timestamp[:]...)
	if r.Tombstone {
		bytes = append(bytes, byte(0))
	} else {
		bytes = append(bytes, byte(1))
	}
	bytes = append(bytes, r.Type)
	bytes, _ = binary.Append(bytes, binary.LittleEndian, r.KeySize)
	bytes, _ = binary.Append(bytes, binary.LittleEndian, r.ValueSize)
	bytes = append(bytes, r.Key...)
	bytes = append(bytes, r.Value...)
	r.CRC = crc32.ChecksumIEEE(bytes)
	bytes = append(binary.LittleEndian.AppendUint32([]byte{}, r.CRC), bytes...)
	return bytes
}

func (r *Record) BytesToRecord(byteptr *[]byte, seek int) (int, bool) {
	if bytes.Equal((*byteptr)[seek:seek+4], []byte{0, 0, 0, 0}) {
		return seek, true
	}
	r.CRC = binary.LittleEndian.Uint32((*byteptr)[seek : seek+4])
	seek += 4
	copy(r.Timestamp[:], (*byteptr)[seek:seek+16])
	seek += 16
	if (*byteptr)[seek] == 0 {
		r.Tombstone = true
	} else {
		r.Tombstone = false
	}
	seek += 1
	r.Type = (*byteptr)[seek]
	seek += 1
	r.KeySize = binary.LittleEndian.Uint64((*byteptr)[seek : seek+8])
	seek += 8
	r.ValueSize = binary.LittleEndian.Uint64((*byteptr)[seek : seek+8])
	seek += 8
	r.Key = (*byteptr)[seek : seek+int(r.KeySize)]
	seek += int(r.KeySize)
	r.Value = (*byteptr)[seek : seek+int(r.ValueSize)]
	seek += int(r.ValueSize)
	return seek, false
}

// NewWAL kreira novu instancu WAL-a
func NewWAL(dirPath string, walMaxRecordsPerSegment int, walBlocksPerSegment int,
	blockSize int, blockCacheSize int) (*WAL, error) {
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return nil, err
	}

	newBM := blockmanager.NewBlockManager(blockSize, blockCacheSize)

	// Pronadji najveci i najmanji broj segmenta
	orderedFiles := make(map[uint32]string)
	sizes := make(map[uint32]int)
	segNum := uint32(0)
	var last uint32
	var first uint32
	first = math.MaxUint32
	last = uint32(0)
	contents, _ := os.ReadDir(dirPath)
	for _, f := range contents {
		if !f.IsDir() {
			block, err := newBM.ReadBlock(filepath.Join(dirPath, f.Name()), 0)
			if err == nil {
				segNum = binary.LittleEndian.Uint32(block[3:7])
				if string(block[:3]) == "WAL" {
					orderedFiles[segNum] = f.Name()
					sizes[segNum] = walBlocksPerSegment
					if segNum > last {
						last = segNum
					}
					if segNum < first {
						first = segNum
					}
				}
			}
		}
	}
	if first == math.MaxUint32 {
		first = 0
	}
	if len(orderedFiles) == 0 {
		orderedFiles[segNum] = fmt.Sprintf("wal_%04d.log", segNum)
	}
	buf := make([]byte, 0, blockSize)
	// Čitanje broja blokova poslednjeg fajla
	file, _ := os.OpenFile(filepath.Join(dirPath, orderedFiles[last]), os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	fileinfo, _ := file.Stat()
	if fileinfo.Size() == 0 {
		buf = append(buf, []byte("WAL")...)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(last))
		newBM.WriteBlock(filepath.Join(dirPath, orderedFiles[last]), buf)
		sizes[segNum] = 1
		newBM.Block_idx = 0
	} else {
		sizes[segNum] = int(fileinfo.Size()) / blockSize
	}
	file.Close()
	// Vrati instancu WAL-a
	return &WAL{
		bm:                  newBM,
		Dir:                 dirPath,
		segments:            orderedFiles,
		sizes:               sizes,
		buffer:              buf,
		walBlocksPerSegment: walBlocksPerSegment,
		blockSize:           blockSize,
		LastSeg:             last,
		FirstSeg:            first,
	}, nil
}

// AppendRecord upisuje zapis u WAL
func (w *WAL) AppendRecord(tombstone bool, key, value []byte) ([16]byte, error) {
	record := Record{
		Tombstone: tombstone,
		KeySize:   uint64(len(key)),
		ValueSize: uint64(len(value)),
		Key:       key,
		Value:     value,
	}

	// Postavi time-stamp
	ts := binary.LittleEndian.AppendUint64([]byte{}, uint64(time.Now().UnixNano()))
	ts = append(ts, make([]byte, 8)...)
	copy(record.Timestamp[:], ts[:])
	// Radimo u petlji - tražimo mesto
	for {
		blockSpace := w.blockSize - len(w.buffer)
		// Ako se uklapa, odmah upisujemo
		if blockSpace >= record.CalculateSize() {
			record.Type = 0
			w.buffer = append(w.buffer, record.RecordToBytes()...)
			return record.Timestamp, nil
		} else {
			// Ako ne može da stane header - upisujemo padding na ostatak bloka
			if blockSpace < 38 {
				w.bm.Block_idx = w.sizes[w.LastSeg] - 1
				w.bm.WriteBlock(filepath.Join(w.Dir, w.segments[w.LastSeg]), w.buffer)
				w.buffer = make([]byte, 0)
				w.sizes[w.LastSeg]++
				if w.bm.Block_idx == w.walBlocksPerSegment {
					w.rotateSegment()
				}
			} else {
				// Ako može - vršimo segmentaciju zapisa
				segments := w.SegmentRecord(record, blockSpace)
				for i := range segments {
					w.buffer = append(w.buffer, segments[i]...)
					if len(w.buffer) == w.blockSize {
						w.bm.WriteBlock(filepath.Join(w.Dir, w.segments[w.LastSeg]), w.buffer)
						w.sizes[w.LastSeg]++
						w.buffer = make([]byte, 0)
						if w.bm.Block_idx == w.walBlocksPerSegment {
							w.rotateSegment()
						}
					}
				}
				return record.Timestamp, nil
			}
		}
	}
}

// Funkcija koja računa koliko je segmenata potrebno za jedan duži zapis i kreira ih
func (w *WAL) SegmentRecord(rec Record, blockSpace int) [][]byte {
	segBytes := make([][]byte, 0)
	seglens := make([]int, 0)
	keyvalLength := int(rec.KeySize + rec.ValueSize)
	keyvalLength -= blockSpace - 38
	seglens = append(seglens, blockSpace-38)
	i := 0
	for keyvalLength > 0 {
		// Vodimo računa koji blok je na početku novog fajla i sadrži header
		if (i+w.bm.Block_idx+1)%w.walBlocksPerSegment == 0 {
			if keyvalLength < w.blockSize-7-38 {
				seglens = append(seglens, keyvalLength)
				keyvalLength = 0
			} else {
				seglens = append(seglens, w.blockSize-7-38)
				keyvalLength = keyvalLength - w.blockSize + 7 + 38
			}
		} else {
			if keyvalLength < w.blockSize-38 {
				seglens = append(seglens, keyvalLength)
				keyvalLength = 0
			} else {
				seglens = append(seglens, w.blockSize-38)
				keyvalLength = keyvalLength - w.blockSize + 38
			}
		}
		i++
	}
	keyIndex := 0
	valueIndex := 0
	for i := 0; i < len(seglens); i++ {
		newRec := Record{
			Tombstone: rec.Tombstone,
		}
		if seglens[i] < int(rec.KeySize)-keyIndex {
			newRec.KeySize = uint64(seglens[i])
			newRec.Key = rec.Key[keyIndex : keyIndex+seglens[i]]
			keyIndex += seglens[i]
			seglens[i] = 0
		} else {
			newRec.KeySize = rec.KeySize - uint64(keyIndex)
			newRec.Key = rec.Key[keyIndex:rec.KeySize]
			keyIndex = int(rec.KeySize)
			seglens[i] -= int(newRec.KeySize)
		}
		if seglens[i] < int(rec.ValueSize)-valueIndex {
			newRec.ValueSize = uint64(seglens[i])
			newRec.Value = rec.Value[valueIndex : valueIndex+seglens[i]]
			valueIndex += seglens[i]
			seglens[i] = 0
		} else {
			newRec.ValueSize = rec.ValueSize - uint64(valueIndex)
			newRec.Value = rec.Value[valueIndex:rec.ValueSize]
			valueIndex = int(rec.ValueSize)
			seglens[i] -= int(newRec.ValueSize)
		}
		if i == 0 {
			newRec.Type = 1
		} else if i == len(seglens)-1 {
			newRec.Type = 3
		} else {
			newRec.Type = 2
		}
		copy(newRec.Timestamp[:], rec.Timestamp[:])
		segBytes = append(segBytes, newRec.RecordToBytes())
	}
	return segBytes
}

// rotateSegment kreira novi segmentni fajl
func (w *WAL) rotateSegment() error {
	// Kreiraj novi segment
	w.LastSeg++
	newPath := filepath.Join(w.Dir, fmt.Sprintf("wal_%04d.log", w.LastSeg))
	file, err := os.OpenFile(newPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Dodaj novi segment u spisak segmenata
	w.segments[w.LastSeg] = fmt.Sprintf("wal_%04d.log", w.LastSeg)
	w.sizes[w.LastSeg] = 1

	// Resetuj sve vrijednosti
	w.buffer = make([]byte, 0, w.blockSize)
	w.buffer = append(w.buffer, []byte("WAL")...)
	w.buffer = binary.LittleEndian.AppendUint32(w.buffer, uint32(w.LastSeg))
	w.bm.Block_idx = 0
	w.bm.WriteBlock(filepath.Join(w.Dir, w.segments[w.LastSeg]), w.buffer)
	w.bm.Block_idx = 0
	return nil
}

// ReadRecords čita sve segmente iz WAL i stavlja u buffer nepopunjeni blok
func (w *WAL) ReadRecords() (map[uint32][]Record, error) {
	recordMap := make(map[uint32][]Record, 0)
	var partialRecord *Record = nil

	// Prođi kroz svaki segment
	currentSeg := w.FirstSeg
	for currentSeg <= w.LastSeg {
		// Prođi kroz svaki blok
		records := make([]Record, 0)
		currentBlock := 0
		for currentBlock < w.sizes[uint32(currentSeg)] {
			block, err := w.bm.ReadBlock(filepath.Join(w.Dir, w.segments[uint32(currentSeg)]), currentBlock)
			if err != nil {
				return nil, err
			}
			seek := 0
			if currentBlock == 0 {
				seek += 7 // Preskoči "WAL" header
			}

			// Ako je preostalo dovoljno prostora za header, čitaj zapis (u suprotnom znamo da je ostatak bloka prazan)
			for len(block)-seek >= 38 {
				newRecord := Record{}
				newseek, end := newRecord.BytesToRecord(&block, seek)
				if end {
					w.buffer = block[:newseek]
					recordMap[w.LastSeg] = records
					return recordMap, nil
				}

				crc := crc32.ChecksumIEEE(block[seek+4 : newseek])
				if crc != newRecord.CRC {
					if newRecord.Type != 0 {
						partialRecord = nil
					}
					seek = newseek
					continue
				}

				switch newRecord.Type {
				case 0: // FULL
					// Dodaj zapis u records
					records = append(records, newRecord)

				case 1: // FIRST
					// Započi rekonstrukciju partialRecord-a
					partialRecord = &Record{
						Timestamp: newRecord.Timestamp,
						Tombstone: newRecord.Tombstone,
						Key:       append([]byte{}, newRecord.Key...),
						Value:     append([]byte{}, newRecord.Value...),
					}

				case 2: // MIDDLE
					// Dodaj fragmente na već postojeci ključ i/ili vrijednost
					if partialRecord != nil {
						partialRecord.Key = append(partialRecord.Key, newRecord.Key...)
						partialRecord.Value = append(partialRecord.Value, newRecord.Value...)
					} else {
						partialRecord = nil
					}

				case 3: // LAST
					// Završi rekonstrukciju partialRecord-a
					if partialRecord != nil {
						partialRecord.Key = append(partialRecord.Key, newRecord.Key...)
						partialRecord.Value = append(partialRecord.Value, newRecord.Value...)

						finalRec := Record{
							Timestamp: partialRecord.Timestamp,
							Tombstone: partialRecord.Tombstone,
							Type:      0,
							KeySize:   uint64(len(partialRecord.Key)),
							ValueSize: uint64(len(partialRecord.Value)),
							Key:       partialRecord.Key,
							Value:     partialRecord.Value,
						}
						finalRec.RecordToBytes()

						// Dodaj rekonstruisani zapis u records
						records = append(records, finalRec)
						partialRecord = nil
					}

				default:
					// Nepoznat tip – ignoriši
					partialRecord = nil
				}

				seek = newseek
			}

			if currentBlock == w.sizes[uint32(currentSeg)]-1 {
				w.buffer = block[:seek]
			}
			currentBlock += 1
		}
		recordMap[currentSeg] = records
		currentSeg += 1
	}

	return recordMap, nil
}

func (w *WAL) WriteOnExit() {
	w.bm.Block_idx = w.sizes[w.LastSeg] - 1
	w.bm.WriteBlock(filepath.Join(w.Dir, w.segments[w.LastSeg]), w.buffer)
}

func (w *WAL) GetSegmentFilename(index uint32) string {
	return w.segments[index]
}
