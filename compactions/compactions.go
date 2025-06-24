package compactions

import (
	"bytes"
	"encoding/binary"
	"projekat/structs/blockmanager"
	"projekat/structs/sstable"
)

func SizeTieredCompaction(tables []*sstable.SSTable, summaries []*sstable.Summary,
	blockSize int, bm *blockmanager.BlockManager, dir string, step int) (*sstable.SSTable, error) {
	recordMatrix := make([][]*sstable.Record, len(tables))
	// Parsiranje svih zapisa u tabeli
	for i := range tables {
		records := make([]*sstable.Record, 0)
		maxKey := summaries[i].MaxKey
		offset := int64(0)
		for {
			record, length, err := sstable.ReadRecordAtOffset(bm, tables[i].DataFilePath, offset, blockSize)
			if err != nil {
				return nil, err
			}
			records = append(records, record)
			if bytes.Equal(record.Key, maxKey) {
				break
			}
			offset += int64(length)
		}
		recordMatrix = append(recordMatrix, records)
	}
	indexes := make([]int, len(tables))
	sortedRecords := make([]sstable.Record, 0)
	for {
		// Maksimalna vrednost za string, iteriramo i tražimo najmanju vrednost
		nextKey := "\xff"
		for i, index := range indexes {
			if index == len(recordMatrix[i]) {
				continue
			}
			if string(recordMatrix[i][index].Key) < nextKey {
				nextKey = string(recordMatrix[i][index].Key)
			}
		}
		// Ovo je moguće samo ako preskočimo sve indekse - sve zapise smo prošli
		if nextKey == "\xff" {
			break
		}
		var nextRecord *sstable.Record = nil
		for i, index := range indexes {
			if nextKey == string(recordMatrix[i][index].Key) {
				if nextRecord == nil {
					nextRecord = recordMatrix[i][index]
					// Ako imaju isti ključ - poredimo timestampove
				} else if binary.LittleEndian.Uint64(nextRecord.Timestamp[:8]) < binary.LittleEndian.Uint64(recordMatrix[i][index].Timestamp[:8]) {
					nextRecord = recordMatrix[i][index]
				}
				indexes[i]++
			}
		}
		sortedRecords = append(sortedRecords, *nextRecord)
	}
	compacted, err := sstable.CreateSSTable(sortedRecords, dir, step, bm, blockSize)
	if err != nil {
		return nil, err
	}
	return compacted, nil
}
