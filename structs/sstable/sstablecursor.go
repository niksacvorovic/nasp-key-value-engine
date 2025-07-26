package sstable

import (
	"fmt"
	"os"
	"projekat/structs/blockmanager"
)

type SSTableCursor struct {
	bm          *blockmanager.BlockManager
	sst         *SSTable
	current     *Record
	minKey      string
	maxKey      string
	offset      int
	blockSize   int
	compression bool
	dict        *Dictionary
}

func NewCursor(bm *blockmanager.BlockManager, path string, minKey string, maxKey string, blockSize int, compression bool, dict *Dictionary) (SSTableCursor, error) {
	sst, err := ReadTableFromDir(path)
	if err != nil {
		fmt.Printf("Greška prilikom čitanja SSTable.")
	}
	sum, err := ReadSummaryFromTable(sst, bm, blockSize)
	if err != nil {
		fmt.Printf("Greška prilikom čitanja SSTable.")
	}
	if string(sum.MaxKey) < minKey || string(sum.MinKey) > maxKey {

	}
	var indices []Index
	var offset int
	if sst.SingleSSTable {
		offsets, err := parseHeader(bm, sst.SingleFilePath, blockSize)
		if err != nil {
			return SSTableCursor{}, err
		}

		idxOff, bound := FindIndexBlockOffset(*sum, []byte(minKey), offsets[2])
		indexLen := bound - idxOff
		idxOff += offsets[1]

		indices, err = ReadIndexBlockSingleFile(bm, sst.SingleFilePath, idxOff, indexLen, blockSize)
		if err != nil {
			return SSTableCursor{}, err
		}

	} else {
		indexInfo, err := os.Stat(sst.IndexFilePath)
		if err != nil {
			return SSTableCursor{}, err
		}
		idxOff, bound := FindIndexBlockOffset(*sum, []byte(minKey), indexInfo.Size())
		indexLen := bound - idxOff
		indices, err = ReadIndexBlock(bm, sst.IndexFilePath, idxOff, indexLen, blockSize)
		if err != nil {
			return SSTableCursor{}, err
		}
	}
	for _, rec := range indices {
		if string(rec.Key) >= minKey {
			break
		}
		offset = int(rec.Offset)
	}
	// Trebamo biti na početku Data segmenta - preskačemo header u SingleFileSSTable
	if sst.SingleSSTable {
		offset += 48
	}
	return SSTableCursor{
		bm:          bm,
		sst:         sst,
		current:     nil,
		minKey:      minKey,
		maxKey:      maxKey,
		offset:      offset,
		blockSize:   blockSize,
		compression: compression,
		dict:        dict,
	}, nil
}

// SSTableCursor se inicijalizuje s vrednošču koja ili pripada opsegu ili je prva pre opsega
func (sc *SSTableCursor) Seek(seekKey string) bool {
	var offsetDelta int
	var err error
	if sc.sst.SingleSSTable {
		sc.current, offsetDelta, err = ReadRecordAtOffsetSingleFile(sc.bm, sc.sst.SingleFilePath, int64(sc.offset), sc.blockSize, sc.compression, sc.dict)
		if err != nil {
			return false
		}
	} else {
		sc.current, offsetDelta, err = ReadRecordAtOffset(sc.bm, sc.sst.DataFilePath, int64(sc.offset), sc.blockSize, sc.compression, sc.dict)
		if err != nil {
			return false
		}
	}
	if string(sc.current.Key) < sc.minKey {
		sc.offset += offsetDelta
	}
	return true

}

func (sc *SSTableCursor) Next() bool {
	var offsetDelta int
	var err error
	if sc.sst.SingleSSTable {
		sc.current, offsetDelta, err = ReadRecordAtOffsetSingleFile(sc.bm, sc.sst.SingleFilePath, int64(sc.offset), sc.blockSize, sc.compression, sc.dict)
		if err != nil {
			return false
		}
	} else {
		sc.current, offsetDelta, err = ReadRecordAtOffset(sc.bm, sc.sst.DataFilePath, int64(sc.offset), sc.blockSize, sc.compression, sc.dict)
		if err != nil {
			return false
		}
	}
	if string(sc.current.Key) > sc.maxKey {
		return false
	}
	sc.offset += offsetDelta
	return true
}

func (sc *SSTableCursor) Key() string {
	if sc.current == nil {
		return ""
	}
	return string(sc.current.Key)
}

func (sc *SSTableCursor) Value() []byte {
	if sc.current == nil {
		return nil
	}
	return sc.current.Value
}

func (sc *SSTableCursor) Timestamp() [16]byte {
	if sc.current == nil {
		return [16]byte{}
	}
	return sc.current.Timestamp
}

func (sc *SSTableCursor) Tombstone() bool {
	if sc.current == nil {
		return false
	}
	return sc.current.Tombstone
}

func (sc *SSTableCursor) Close() {
	sc.offset = -1
	sc.current = nil
	sc.bm = nil
	sc.sst = nil
}
