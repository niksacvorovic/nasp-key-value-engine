package sstable

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"projekat/structs/blockmanager"
	"slices"
	"strings"
)

func ReadTableFromDir(subdirPath string) (*SSTable, error) {
	files, err := os.ReadDir(subdirPath)
	if err != nil {
		return nil, err
	}
	if len(files) == 1 {
		filePath := filepath.Join(subdirPath, files[0].Name())
		return &SSTable{SingleSSTable: true, SingleFilePath: filePath}, nil
	} else {
		sst := &SSTable{SingleSSTable: false}
		for _, f := range files {
			if strings.HasSuffix(f.Name(), "Data.db") {
				sst.DataFilePath = filepath.Join(subdirPath, f.Name())
			}
			if strings.HasSuffix(f.Name(), "Index.db") {
				sst.IndexFilePath = filepath.Join(subdirPath, f.Name())
			}
			if strings.HasSuffix(f.Name(), "Summary.db") {
				sst.SummaryFilePath = filepath.Join(subdirPath, f.Name())
			}
			if strings.HasSuffix(f.Name(), "Filter.db") {
				sst.FilterFilePath = filepath.Join(subdirPath, f.Name())
			}
			if strings.HasSuffix(f.Name(), "Metadata.db") {
				sst.MetadataFilePath = filepath.Join(subdirPath, f.Name())
			}
		}
		return sst, nil
	}
}

func ReadSummaryFromTable(sst *SSTable, bm *blockmanager.BlockManager, blockSize int) (*Summary, error) {
	var sum Summary
	var err error = nil
	if sst.SingleSSTable {
		offsets, err := parseHeader(bm, sst.SingleFilePath, blockSize)
		if err != nil {
			return nil, err
		}
		sum, err = LoadSummarySingleFile(bm, sst.SingleFilePath, blockSize, offsets[2], offsets[3])
		if err != nil {
			return nil, err
		}
	} else {
		sum, err = LoadSummary(bm, sst.SummaryFilePath)
		if err != nil {
			return nil, err
		}
	}
	return &sum, err
}

func Compaction(tables []*SSTable, blockSize int, bm *blockmanager.BlockManager,
	dir string, step int, single bool, lsm byte, compress bool, dict *Dictionary, dictPath string) (*SSTable, string, error) {

	recordMatrix := make([][]*Record, len(tables))
	// Učitavanje svih Summary-ja
	summaries := make([]*Summary, len(tables))
	for i := range tables {
		sum, err := ReadSummaryFromTable(tables[i], bm, blockSize)
		if err != nil {
			return nil, "", err
		}
		summaries[i] = sum
	}
	// Parsiranje svih zapisa u tabeli
	for i := range tables {
		records := make([]*Record, 0)
		maxKey := summaries[i].MaxKey
		offset := int64(0)
		var path string
		if tables[i].SingleSSTable {
			path = tables[i].SingleFilePath
			offset += 48
		} else {
			path = tables[i].DataFilePath
		}
		for {
			record, length, err := ReadRecordAtOffset(bm, path, offset, blockSize, compress, dict)
			if err != nil {
				return nil, "", err
			}
			records = append(records, record)
			if bytes.Equal(record.Key, maxKey) {
				break
			}
			offset += int64(length)
		}
		recordMatrix[i] = records
	}

	cursors := make([]int, len(tables))
	sortedRecords := make([]Record, 0)
	for {
		// Maksimalna vrednost za string, iteriramo i tražimo najmanju vrednost
		nextKey := "\xff"
		for i, cursor := range cursors {
			if cursor == len(recordMatrix[i]) {
				continue
			}
			if string(recordMatrix[i][cursor].Key) < nextKey {
				nextKey = string(recordMatrix[i][cursor].Key)
			}
		}
		// Ovo je moguće samo ako preskočimo sve indekse - sve zapise smo prošli
		if nextKey == "\xff" {
			break
		}
		var nextRecord *Record = nil
		for i, cursor := range cursors {
			if cursor == len(recordMatrix[i]) {
				continue
			}
			if nextKey == string(recordMatrix[i][cursor].Key) {
				if nextRecord == nil {
					nextRecord = recordMatrix[i][cursor]
					// Ako imaju isti ključ - poredimo timestampove
				} else if binary.LittleEndian.Uint64(nextRecord.Timestamp[:8]) < binary.LittleEndian.Uint64(recordMatrix[i][cursor].Timestamp[:8]) {
					nextRecord = recordMatrix[i][cursor]
				}
				cursors[i]++
			}
		}
		// Zapisi koji su obrisani se preskaču - fizičko brisanje
		if nextRecord.Tombstone {
			continue
		}
		sortedRecords = append(sortedRecords, *nextRecord)
	}
	compacted, sstDir, err := CreateSSTable(sortedRecords, dir, step, bm, blockSize, lsm, single, compress, dict, dictPath)
	if err != nil {
		return nil, "", err
	}
	return compacted, sstDir, nil
}

func CheckLSMLevels(bm *blockmanager.BlockManager, dirPath string, blockSize int) (map[byte][]string, error) {
	levelsMap := make(map[byte][]string)
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
				offsets, err := parseHeader(bm, filePath, blockSize)
				if err != nil {
					return nil, err
				}
				summary, err := LoadSummarySingleFile(bm, filePath, blockSize, offsets[2], offsets[3])
				if err != nil {
					return nil, err
				}
				_, ok := levelsMap[summary.Compaction]
				if !ok {
					levelsMap[summary.Compaction] = make([]string, 0)
				}
				levelsMap[summary.Compaction] = append(levelsMap[summary.Compaction], subDirPath)
			} else {
				for _, f := range files {
					if strings.HasSuffix(f.Name(), "Summary.db") {
						summaryPath := filepath.Join(subDirPath, f.Name())
						summary, err := LoadSummary(bm, summaryPath)
						if err != nil {
							return nil, err
						}
						_, ok := levelsMap[summary.Compaction]
						if !ok {
							levelsMap[summary.Compaction] = make([]string, 0)
						}
						levelsMap[summary.Compaction] = append(levelsMap[summary.Compaction], subDirPath)
					}
				}
			}
		}
	}
	return levelsMap, nil
}

func SizeTieredCompaction(bm *blockmanager.BlockManager, lsm *map[byte][]string, dirPath string,
	maxInLevel int, blockSize int, step int, single bool, compression bool, dict *Dictionary, dictPath string) error {
	loop := true
	for loop {
		loop = false
		for k, level := range *lsm {
			if len(level) > maxInLevel {
				tables := make([]*SSTable, 0)
				loop = true
				for _, subdirPath := range level {
					table, err := ReadTableFromDir(subdirPath)
					if err != nil {
						return nil
					}
					tables = append(tables, table)
				}
				_, sstDir, err := Compaction(tables, blockSize, bm, dirPath, step, single, k+1, compression, dict, dictPath)
				if err != nil {
					return err
				}
				// Brisanje SSTabela koje su kompaktovane
				for _, path := range level {
					err := os.RemoveAll(path)
					if err != nil {
						return err
					}
				}
				// Apdejt LSM stabla
				(*lsm)[k] = make([]string, 0)
				_, ok := (*lsm)[k+1]
				if !ok {
					(*lsm)[k+1] = make([]string, 0)
				}
				(*lsm)[k+1] = append((*lsm)[k+1], sstDir)
			}
		}
	}
	return nil
}

func moveToLowerLevel(levelDir string, bm *blockmanager.BlockManager, blockSize int, lvl byte) error {
	files, err := os.ReadDir(levelDir)
	if err != nil {
		return err
	}
	if len(files) == 1 {
		filePath := filepath.Join(levelDir, files[0].Name())
		offsets, err := parseHeader(bm, filePath, blockSize)
		if err != nil {
			return err
		}
		summaryBlock, err := bm.ReadBlock(filePath, int(offsets[2])/blockSize)
		if err != nil {
			return nil
		}
		summaryBlock[offsets[2]%int64(blockSize)] = lvl
		bm.Block_idx = int(offsets[2]) / blockSize
		err = bm.WriteBlock(filePath, summaryBlock)
		if err != nil {
			return err
		}
	} else {
		for _, f := range files {
			if strings.HasSuffix(f.Name(), "Summary.db") {
				summaryPath := filepath.Join(levelDir, f.Name())
				summaryBlock, err := bm.ReadBlock(summaryPath, 0)
				if err != nil {
					return err
				}
				summaryBlock[0] = lvl
				bm.Block_idx = 0
				err = bm.WriteBlock(summaryPath, summaryBlock)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func LeveledCompaction(bm *blockmanager.BlockManager, lsm *map[byte][]string, dirPath string,
	maxInLevel int, blockSize int, step int, single bool, compression bool, dict *Dictionary, dictPath string) error {
	loop := true
	for loop {
		loop = false
		for k, level := range *lsm {
			leveledMax := maxInLevel
			for i := byte(0); i < k; i++ {
				leveledMax *= 10
			}
			if len(level) > leveledMax {
				tables := make([]*SSTable, 0)
				loop = true
				nextLevel, ok := (*lsm)[k+1]
				if ok {
					// Kompakcija ukoliko na sledećem nivou ima tabeli
					compactedDirs := make([]string, 0)
					compactedDirs = append(compactedDirs, level[0])
					uppersst, err := ReadTableFromDir(level[0])
					if err != nil {
						return err
					}
					(*lsm)[k] = (*lsm)[k][1:]
					tables = append(tables, uppersst)
					upperSummary, err := ReadSummaryFromTable(uppersst, bm, blockSize)
					minStr := string(upperSummary.MinKey)
					maxStr := string(upperSummary.MaxKey)
					if err != nil {
						return err
					}
					for _, lowerDir := range nextLevel {
						lowersst, err := ReadTableFromDir(lowerDir)
						if err != nil {
							return err
						}
						lowerSummary, err := ReadSummaryFromTable(lowersst, bm, blockSize)
						if err != nil {
							return err
						}
						// Opsezi se ne preklapaju - preskačemo
						if minStr > string(lowerSummary.MaxKey) || maxStr < string(lowerSummary.MinKey) {
							continue
							// Opseg gornje tabele je unutar druge - kompaktujemo samo njih dve
						} else if string(lowerSummary.MaxKey) > maxStr && string(lowerSummary.MinKey) < minStr {
							tables = append(tables, lowersst)
							compactedDirs = append(compactedDirs, lowerDir)
							break
						} else {
							tables = append(tables, lowersst)
							compactedDirs = append(compactedDirs, lowerDir)
						}
					}
					// Nijedan opseg se ne poklapa - pomeramo SSTabelu na sledeći nivo
					if len(compactedDirs) == 1 {
						err := moveToLowerLevel(compactedDirs[0], bm, blockSize, k+1)
						if err != nil {
							return nil
						}
						(*lsm)[k+1] = append((*lsm)[k+1], compactedDirs[0])
						// Opsezi se poklapaju - kompaktujemo sve pohvatane table
					} else {
						_, newPath, err := Compaction(tables, blockSize, bm, dirPath, step, single, k+1, compression, dict, dictPath)
						if err != nil {
							return err
						}
						// Uklanjamo sve kompaktovane tabele i izbacujemo ih iz LSM stabla
						for _, p := range compactedDirs {
							err := os.RemoveAll(p)
							if err != nil {
								return nil
							}
						}
						for _, p := range compactedDirs[1:] {
							for j, oldPath := range (*lsm)[k+1] {
								if p == oldPath {
									(*lsm)[k+1] = slices.Delete((*lsm)[k+1], j, j+1)
									break
								}
							}
						}
						(*lsm)[k+1] = append((*lsm)[k+1], newPath)
					}
				} else {
					// Ako nema, prebacujemo jednu tabelu u sledeći nivo
					err := moveToLowerLevel(level[0], bm, blockSize, k+1)
					if err != nil {
						return err
					}
					// Dodajemo novi nivo LSM stablu a iz starog izbacujemo nulti element
					(*lsm)[k+1] = make([]string, 0)
					(*lsm)[k+1] = append((*lsm)[k+1], level[0])
					(*lsm)[k] = (*lsm)[k][1:]
				}
			}
		}
	}
	return nil
}
