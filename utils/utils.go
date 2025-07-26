package utils

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"projekat/config"
	"projekat/structs/blockmanager"
	"projekat/structs/memtable"
	"projekat/structs/sstable"
	"projekat/structs/wal"
	"strings"
)

// Funkcija za parsiranje korisnickih komandi
func ParseArgs(input string) ([]string, error) {
	var parts []string
	var current strings.Builder
	inQuotes := false
	escapeNext := false

	// Iteriramo kroz sve karaktere ulaznog stringa
	for i := 0; i < len(input); i++ {
		c := input[i]

		switch {
		// Ako je prethodni karakter bio '\' karakter se odma dodaje
		case escapeNext:
			current.WriteByte(c)
			escapeNext = false

		// Ako je trenutni karakter '\' sledeci karakter treba biti escepovan
		case c == '\\':
			escapeNext = true

		// Ako je karakter navodnik ukljucujemo/iskljucujemo mod navodnika
		case c == '"':
			inQuotes = !inQuotes

		// Ako je razmak ili tab
		case c == ' ' || c == '\t':
			if inQuotes {
				// Ako smo unutar navodnika razmak/tab je dio dijela komande
				current.WriteByte(c)
			} else if current.Len() > 0 {
				// Dodajemo u listu dijelova komande
				parts = append(parts, current.String())
				current.Reset()
			}

		default:
			current.WriteByte(c)
		}
	}

	// Greska ako su navodnici ostali otvoreni
	if inQuotes {
		return nil, fmt.Errorf("nezatvoreni navodnici u komandi")
	}

	// Dodaj i poslednji argument ako postoji
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}

	return parts, nil
}

// Funkcija za spremanje ispisa
func MaybeQuote(s string) string {
	if strings.ContainsAny(s, " \t\"") {
		escaped := strings.ReplaceAll(s, `"`, `\"`)
		return `"` + escaped + `"`
	}
	return s
}

func WriteToMemory(ts [16]byte, tombstone bool, key string, value []byte, bm *blockmanager.BlockManager,
	memtables *[]memtable.MemtableInterface, mtIndex *int, wal *wal.WAL, mtnum int) *[]sstable.Record {
	for i := range *memtables {
		_, _, exists := (*memtables)[i].Get(key)
		if exists {
			(*memtables)[i].Add(ts, tombstone, key, value)
		}
	}
	(*memtables)[*mtIndex].Add(ts, tombstone, key, value)
	fmt.Printf("Uspešno dodato: [%s -> %s]\n", MaybeQuote(string(key)), MaybeQuote(string(value)))
	(*memtables)[*mtIndex].SetWatermark(wal.LastSeg)
	// Proveravamo da li je trenutni memtable popunjen
	if (*memtables)[*mtIndex].IsFull() {
		fmt.Println("Dostignuta maksimalna veličina Memtable-a, prelazim na sledeći...")
		*mtIndex = (*mtIndex + 1) % mtnum
		// Ako je i sledeći memtable pun - svi su puni
		// Flushujemo memtable i stavljamo njegov sadržaj u SSTable
		if (*memtables)[*mtIndex].IsFull() {
			// Low watermark provera za brisanje WALa
			// Brišemo sve WAL segmente između trenutno najstarijeg i watermarka
			watermark := (*memtables)[*mtIndex].GetWatermark()
			for i := wal.FirstSeg; i < watermark; i++ {
				deletePath := wal.GetSegmentFilename(i)
				err := os.Remove(filepath.Join(wal.Dir, deletePath))
				if err != nil {
					fmt.Printf("Greška pri brisanju WAL segmenata")
				}
			}
			wal.FirstSeg = watermark
			fmt.Println("Prevodim sadržaj Memtable-a u SSTable")
			sstrecords := memtable.ConvertMemToSST(&(*memtables)[*mtIndex])
			return sstrecords
		}
	}
	return nil
}

func WriteToDisk(sstrecords *[]sstable.Record, sstableDir string, bm *blockmanager.BlockManager,
	lsm *map[byte][]string, cfg config.Config, dict *sstable.Dictionary, dictPath string) error {
	_, newSSTdir, err := sstable.CreateSSTable(*sstrecords, sstableDir, cfg.SummaryStep, bm, cfg.BlockSize,
		0, cfg.SSTableSingleFile, cfg.SSTableCompression, dict, dictPath)
	if err != nil {
		return err
	}
	(*lsm)[0] = append((*lsm)[0], newSSTdir)
	// Funkcija za proveru i izvršenje kompakcija
	switch cfg.CompactionAlgorithm {
	case "SizeTiered":
		err := sstable.SizeTieredCompaction(bm, lsm, sstableDir, cfg.MaxCountInLevel,
			cfg.BlockSize, cfg.SummaryStep, cfg.SSTableSingleFile, cfg.SSTableCompression, dict, dictPath)
		if err != nil {
			return err
		}
	case "Leveled":
		err := sstable.LeveledCompaction(bm, lsm, sstableDir, cfg.MaxCountInLevel,
			cfg.BlockSize, cfg.SummaryStep, cfg.SSTableSingleFile, cfg.SSTableCompression, dict, dictPath)
		if err != nil {
			return err
		}
	}
	fmt.Println("SSTable uspešno kreiran!")
	return nil
}

func ReadFromDisk(key string, maxLevel byte, lsm map[byte][]string, cfg config.Config,
	bm *blockmanager.BlockManager, dict *sstable.Dictionary) *sstable.Record {
	records := make([]*sstable.Record, 0)
	for level := byte(0); level <= maxLevel; level++ {
		sstableDirs, exists := lsm[level]
		if !exists {
			continue
		}

		for _, dir := range sstableDirs {
			table, err := sstable.ReadTableFromDir(dir)
			if err != nil {
				fmt.Println("Greška u čitanju foldera SSTabele")
			}
			record, found := sstable.SearchSSTable(table, key, cfg, bm, dict)
			if found {
				records = append(records, record)
				// u leveled kompakciji podatak se pojavljuje samo jednom u nivou
				// podatak u najvišem nivou je ujedno i najnoviji - možemo ga vratiti odmah
				if cfg.CompactionAlgorithm == "Leveled" {
					return record
				}
			}
		}
	}
	if len(records) == 0 {
		return nil
	}
	retIndex := 0
	for i, rec := range records {
		// vraćamo zapis sa najnovijim timestampom
		if binary.LittleEndian.Uint64(rec.Timestamp[:8]) > binary.LittleEndian.Uint64(records[retIndex].Timestamp[:8]) {
			retIndex = i
		}
	}
	if records[retIndex].Tombstone {
		return nil
	}
	return records[retIndex]
}

// Komande koje trose tokene (sve sem HELP i EXIT)
var CommandsWithTokens = map[string]bool{
	"GET": true, "PUT": true, "DELETE": true,
	"PREFIX_SCAN": true, "RANGE_SCAN": true,
	"PREFIX_ITERATE": true, "RANGE_ITERATE": true,
	"BLOOM_CREATE": true, "BLOOM_ADD": true, "BLOOM_CHECK": true,
	"CMS_CREATE": true, "CMS_ADD": true, "CMS_COUNT": true,
	"HLL_CREATE": true, "HLL_ADD": true, "HLL_COUNT": true,
	"SIMHASH_ADD": true, "SIMHASH_DIST": true,
}
