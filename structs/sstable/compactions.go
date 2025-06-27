package sstable

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"projekat/structs/blockmanager"
	"strings"
)

func Compaction(tables []*SSTable, blockSize int, bm *blockmanager.BlockManager,
	dir string, step int, single bool, lsm byte) (*SSTable, error) {

	recordMatrix := make([][]*Record, len(tables))
	summaries := make([]Summary, len(tables))
	for i := range tables {
		if tables[i].SingleSSTable {
			summary, err := LoadSummary(bm, tables[i].SummaryFilePath)
			if err != nil {
				return nil, err
			}
			summaries = append(summaries, summary)
		} else {
			offsets, err := parseFooter(bm, tables[i].SingleFilePath, blockSize)
			if err != nil {
				return nil, err
			}
			summary, err := LoadSummarySingleFile(bm, tables[i].SingleFilePath, blockSize, offsets[2])
			if err != nil {
				return nil, err
			}
			summaries = append(summaries, summary)
		}
	}
	// Parsiranje svih zapisa u tabeli
	for i := range tables {
		records := make([]*Record, 0)
		maxKey := summaries[i].MaxKey
		offset := int64(0)
		for {
			record, length, err := ReadRecordAtOffset(bm, tables[i].DataFilePath, offset, blockSize)
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
	sortedRecords := make([]Record, 0)
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
		var nextRecord *Record = nil
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
	compacted, err := CreateSSTable(sortedRecords, dir, step, bm, blockSize, lsm, single)
	if err != nil {
		return nil, err
	}
	return compacted, nil
}

func CheckLSMLevels(bm *blockmanager.BlockManager, dirPath string, blockSize int) (map[byte]int, error) {
	levelsMap := make(map[byte]int)
	dir, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}
	// Svaka SSTabela se nalazi u svom posvećenom folderu - bilo da je iz jednog ili više delova
	for _, dirEntry := range dir {
		if dirEntry.IsDir() {
			subDirPath := filepath.Join(dirPath, dirEntry.Name())
			files, err := os.ReadDir(subDirPath)
			if err != nil {
				return nil, err
			}
			if len(files) == 1 {
				filePath := filepath.Join(subDirPath, files[0].Name())
				offsets, err := parseFooter(bm, filePath, blockSize)
				if err != nil {
					return nil, err
				}
				summary, err := LoadSummarySingleFile(bm, filePath, blockSize, offsets[2])
				if err != nil {
					return nil, err
				}
				_, ok := levelsMap[summary.Compaction]
				if ok {
					levelsMap[summary.Compaction]++
				} else {
					levelsMap[summary.Compaction] = 1
				}
			} else {
				for _, f := range files {
					if strings.HasSuffix(f.Name(), "Summary.db") {
						summaryPath := filepath.Join(subDirPath, f.Name())
						summary, err := LoadSummary(bm, summaryPath)
						if err != nil {
							return nil, err
						}
						_, ok := levelsMap[summary.Compaction]
						if ok {
							levelsMap[summary.Compaction]++
						} else {
							levelsMap[summary.Compaction] = 1
						}
					}
				}
			}
		}
	}
	return levelsMap, nil
}

func SizeTieredCompaction(bm *blockmanager.BlockManager, lsmCount map[byte]int, dirPath string,
	maxInLevel int, blockSize int, step int, single bool) error {
	loop := true
	for loop {
		loop = false
		for k, count := range lsmCount {
			if count > maxInLevel {
				tables := make([]*SSTable, 0)
				loop = true
				dir, err := os.ReadDir(dirPath)
				if err != nil {
					return err
				}
			search:
				for _, dirEntry := range dir {
					if dirEntry.IsDir() {
						subDirPath := filepath.Join(dirPath, dirEntry.Name())
						files, err := os.ReadDir(subDirPath)
						if err != nil {
							return err
						}
						if len(files) == 1 {
							filePath := filepath.Join(subDirPath, files[0].Name())
							offsets, err := parseFooter(bm, filePath, blockSize)
							if err != nil {
								return err
							}
							summary, err := LoadSummarySingleFile(bm, filePath, blockSize, offsets[2])
							if err != nil {
								return err
							}
							if summary.Compaction == k {
								tables = append(tables, &SSTable{SingleSSTable: true, SingleFilePath: filePath})
								if len(tables) == count {
									break search
								}
							}
						} else {
							var dataPath string
							for _, f := range files {
								if strings.HasSuffix(f.Name(), "Data.db") {
									dataPath = filepath.Join(subDirPath, f.Name())
								}
							}
							for _, f := range files {
								if strings.HasSuffix(f.Name(), "Summary.db") {
									summaryPath := filepath.Join(subDirPath, f.Name())
									summary, err := LoadSummary(bm, summaryPath)
									if err != nil {
										return err
									}
									if summary.Compaction == k {
										tables = append(tables, &SSTable{SingleSSTable: false,
											SummaryFilePath: summaryPath,
											DataFilePath:    dataPath})
										if len(tables) == count {
											break search
										}
									}
								}
							}
						}
					}
				}
				Compaction(tables, blockSize, bm, dirPath, step, single, k+1)
				lsmCount[k] = 0
				_, ok := lsmCount[k+1]
				if ok {
					lsmCount[k+1]++
				} else {
					lsmCount[k+1] = 1
				}
			}
		}
	}
	return nil
}

func LeveledCompaction(bm *blockmanager.BlockManager, lsmCount map[byte]int, dirPath string,
	maxInLevel int, blockSize int, step int, single bool) error {
	// loop := true
	// for loop {
	// 	loop = false
	// 	for k, count := range lsmCount {
	// 		if count > maxInLevel {
	// 			tables := make([]*SSTable, 0)
	// 			loop = true
	// 			dir, err := os.ReadDir(dirPath)
	// 			if err != nil {
	// 				return err
	// 			}

	// 			lsmCount[k] = 0
	// 			_, ok := lsmCount[k+1]
	// 			if ok {
	// 				lsmCount[k+1]++
	// 			} else {
	// 				lsmCount[k+1] = 1
	// 			}
	// 		}
	// 	}
	// }
	return nil
}
