package sstable

import "projekat/structs/blockmanager"

type SSTableCursor struct {
	bm        *blockmanager.BlockManager
	sst       *SSTable
	current   *Record
	minKey    string
	maxKey    string
	offset    int
	blockSize int
}

func NewCursor(bm *blockmanager.BlockManager, sst *SSTable, minKey string, maxKey string, blockSize int) (SSTableCursor, error) {

	return SSTableCursor{
		bm:        bm,
		sst:       sst,
		current:   nil,
		minKey:    minKey,
		maxKey:    maxKey,
		offset:    0,
		blockSize: blockSize,
	}, nil
}

func (sc *SSTableCursor) Seek(seekKey string) bool {
	var err error
	if sc.sst.SingleSSTable {
		sc.current, sc.offset, err = SearchSingleFile(sc.bm, sc.sst, []byte(sc.minKey), sc.blockSize)
		if err != nil {
			return false
		}
	} else {
		summary, err := LoadSummary(sc.bm, sc.sst.SummaryFilePath)

		if err != nil {
			return false
		}
		sc.current, sc.offset, err = Search(sc.bm, sc.sst, []byte(sc.minKey), summary, int64(sc.blockSize), sc.blockSize)
		if err != nil {
			return false
		}
	}
	return true
}

func (sc *SSTableCursor) Next() bool {
	var offsetDelta int
	var err error
	if sc.sst.SingleSSTable {
		sc.current, offsetDelta, err = ReadRecordAtOffsetSingleFile(sc.bm, sc.sst.SingleFilePath, int64(sc.offset), sc.blockSize)
		if err != nil {
			return false
		}
	} else {
		sc.current, offsetDelta, err = ReadRecordAtOffset(sc.bm, sc.sst.DataFilePath, int64(sc.offset), sc.blockSize)
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
	if sc.current != nil {
		return ""
	}
	return string(sc.current.Key)
}

func (sc *SSTableCursor) Value() []byte {
	if sc.current != nil {
		return nil
	}
	return sc.current.Value
}

func (sc *SSTableCursor) Timestamp() [16]byte {
	if sc.current != nil {
		return [16]byte{}
	}
	return sc.current.Timestamp
}

func (sc *SSTableCursor) Tombstone() bool {
	if sc.current != nil {
		return false
	}
	return sc.current.Tombstone
}

func (sc *SSTableCursor) Close() {
	sc.offset = -1
}
