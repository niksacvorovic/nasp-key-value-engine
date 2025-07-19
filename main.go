package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"projekat/config"
	"projekat/structs/blockmanager"
	"projekat/structs/containers"
	"projekat/structs/cursor"
	"projekat/structs/lrucache"
	"projekat/structs/memtable"
	"projekat/structs/probabilistic"
	"projekat/structs/sstable"
	"projekat/structs/wal"
)

func main() {

	// Citanje config.json fajla
	data, err := os.ReadFile("config/config.json")
	if err != nil {
		log.Fatal(err)
	}

	// Inicijalizuj Config strukturu
	var cfg config.Config

	// Unmarshal JSON podatke u Config strukturu
	err = json.Unmarshal(data, &cfg)
	if err != nil {
		log.Fatal(err)
	}

	// -------------------------------------------------------------------------------------------------------------------------------
	// Block Manager i Block Cache
	// -------------------------------------------------------------------------------------------------------------------------------

	// Inicijalizacija LRU keša
	lru := lrucache.NewLRUCache(cfg.LRUCacheSize)

	// Inicijalizacija globalnog BlockManager
	bm := blockmanager.NewBlockManager(cfg.BlockSize, cfg.BlockCacheSize)

	// -------------------------------------------------------------------------------------------------------------------------------
	// Memtabable
	// -------------------------------------------------------------------------------------------------------------------------------

	// Niz memtable instanci
	memtableInstances := make([]memtable.MemtableInterface, cfg.MemtableNum)
	mtIndex := 0

	// Inicijalizacija niza instanci Memtable-a
	switch cfg.MemtableStruct {
	case "hashMap":
		for i := 0; i < cfg.MemtableNum; i++ {
			memtableInstances[i] = containers.NewHashMapMemtable(cfg.MaxMemtableSize)
		}
	case "skipList":
		for i := 0; i < cfg.MemtableNum; i++ {
			memtableInstances[i] = containers.NewSkipListMemtable(cfg.SkipListLevelNum, cfg.MaxMemtableSize)
		}

	case "BStablo":
		for i := 0; i < cfg.MemtableNum; i++ {
			memtableInstances[i] = containers.NewBTreeMemtable(cfg.MaxMemtableSize)
		}
	}

	// -------------------------------------------------------------------------------------------------------------------------------
	// WAL (Write Ahead Log)
	// -------------------------------------------------------------------------------------------------------------------------------

	// Put do foldera sa wal logovima
	walDir := filepath.Join("data", "wal")

	// Inicijalizacija WAL-a
	walInstance, err := wal.NewWAL(walDir, cfg.WalMaxRecordsPerSegment, cfg.WalBlocksPerSegment, cfg.BlockSize, cfg.BlockCacheSize)
	if err != nil {
		log.Fatalf("Greška pri inicijalizaciji WAL-a: %v", err)
	}

	// Citanje WAL fajlova
	recordMap, err := walInstance.ReadRecords()
	if err != nil {
		fmt.Println("Greška pri čitanju:", err)
	}

	// Dodavanje WAL zapisa u Memtabele
	for w, records := range recordMap {
		for _, rec := range records {
			// Dodavanje zapisa u memtable
			memtableInstances[mtIndex].Add(rec.Timestamp, rec.Tombstone, string(rec.Key), rec.Value)

			// Postavljanje watermarka za svaki Memtable
			memtableInstances[mtIndex].SetWatermark(w)

			// Provera da li je trenutni Memtable pun
			if memtableInstances[mtIndex].IsFull() {
				mtIndex = (mtIndex + 1) % cfg.MemtableNum
			}
		}
	}

	// Čuvanje prvog slobodnog indeksa za Token Bucket
	tokenIndex := mtIndex

	// -------------------------------------------------------------------------------------------------------------------------------
	// SSTable i LSM stablo
	// -------------------------------------------------------------------------------------------------------------------------------

	// Putanja do foldera sa SSTable fajlovima
	sstableDir := filepath.Join("data", "sstable")

	// Putanja do fajla sa globalnim recnikom kompresije
	dictPath := filepath.Join("data", "dictionary.db")

	// Globalni rečnik za SSTable
	dict := sstable.NewDictionaryWithThreshold(cfg.BlockSize)
	_ = dict.LoadFromFile(dictPath, bm, cfg.BlockSize)

	// Prebrojavanje SSTabli na svakom nivou LSM stabla
	var lsm map[byte][]string
	_, err = os.Stat(sstableDir)
	if !os.IsNotExist(err) {
		lsm, err = sstable.CheckLSMLevels(bm, sstableDir, cfg.BlockSize)
		if err != nil {
			fmt.Println("Greška pri pristupu SSTable fajlovima")
		}
	} else {
		lsm = make(map[byte][]string)
		lsm[0] = make([]string, 0)
	}

	// -------------------------------------------------------------------------------------------------------------------------------
	// Interfejs petlja
	// -------------------------------------------------------------------------------------------------------------------------------

	fmt.Println("Dobrodosli u Key-Value Engine!")
	fmt.Println("Unesite komandu (help za listu dostupnih komandi):")

	// Scanner za citanje korisnickih unosa
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("> ")

		// Citanje linije iz inputa
		if !scanner.Scan() {
			break
		}
		input := strings.TrimSpace(scanner.Text())

		// Podela komande na delove
		parts := strings.Fields(input)

		if len(parts) == 0 {
			continue
		}

		// Parsiranje komande
		command := strings.ToUpper(parts[0])

		// Kontrola pristupa
		// Token bucket će se uvek čuvati u prvoj slobodnoj memtabeli pri pokretanju
		if command == "GET" || command == "PUT" || command == "DELETE" {
			bucket, ok := memtableInstances[tokenIndex].Get("__sys__TOKEN_BUCKET")
			if !ok {
				newtimestamp := uint64(time.Now().Unix())
				newtokens := uint8(cfg.TokenRate)
				newbucket := make([]byte, 0)
				newbucket = binary.BigEndian.AppendUint64(newbucket, newtimestamp)
				newbucket = append(newbucket, newtokens)
				memtableInstances[tokenIndex].Add([16]byte{}, false, "__sys__TOKEN_BUCKET", newbucket)
				bucket = newbucket
			}
			timestamp := binary.BigEndian.Uint64(bucket[0:8])
			tokens := uint8(bucket[8])
			if tokens == 0 && time.Now().Unix()-int64(timestamp) < int64(cfg.TokenInterval) {
				fmt.Println("Prekoračen broj tokena! Molim Vas sačekajte...")
				continue
			} else {
				if time.Now().Unix()-int64(timestamp) >= int64(cfg.TokenInterval) {
					timestamp = uint64(time.Now().Unix())
					tokens = uint8(cfg.TokenRate)
				}
				tokens--
				bucket = make([]byte, 0)
				bucket = binary.BigEndian.AppendUint64(bucket, timestamp)
				bucket = append(bucket, tokens)
			}
			memtableInstances[tokenIndex].Add([16]byte{}, false, "__sys__TOKEN_BUCKET", bucket)
		}

		// --------------------------------------------------------------------------------------------------------------------------
		// PUT komanda
		// --------------------------------------------------------------------------------------------------------------------------

		switch command {
		// PUT komanda ocekuje 2 argumenta: key, value
		case "PUT":
			if strings.HasPrefix(parts[1], "__sys__") {
				fmt.Println("Zabranjena operacija nad internim kljucevima.")
				continue
			}

			// Proverava da li PUT komanda ima tačno 2 argumenta: key i value
			if len(parts) != 3 {
				fmt.Println("Greška: PUT zahteva <key> <value>")
				continue
			}

			// Konverzija ključa i vrednosti u []byte tip, tombstone je postavljen na false
			key := []byte(parts[1])
			value := []byte(parts[2])
			tombstone := false

			// Izmena LRU keša (ukoliko je potrebno)
			_, exists := lru.CheckCache(parts[1])
			if exists {
				lru.UpdateCache(parts[1], value)
			}

			// Dodavanje zapisa u Write-Ahead Log (WAL)
			ts, err := walInstance.AppendRecord(tombstone, key, value)
			if err != nil {
				// Ako dodje do greske prilikom upisa u WAL, ispisuje se poruka o gresci
				fmt.Printf("Greška prilikom pisanja u WAL: %v\n", err)
			} else {
				memtableInstances[mtIndex].Add(ts, tombstone, parts[1], value)
				memtableInstances[mtIndex].SetWatermark(walInstance.LastSeg)
				// Proveravamo da li je trenutni memtable popunjen
				if memtableInstances[mtIndex].IsFull() {
					fmt.Println("Dostignuta maksimalna veličina Memtable-a, prelazak na sledeći...")
					mtIndex = (mtIndex + 1) % cfg.MemtableNum
					// Ako je i sledeći memtable pun - svi su puni
					// Flushujemo memtable i stavljamo njegov sadržaj u SSTable
					if memtableInstances[mtIndex].IsFull() {
						// Low watermark provera za brisanje WALa
						// Brišemo sve WAL segmente između trenutno najstarijeg i watermarka
						watermark := memtableInstances[mtIndex].GetWatermark()
						for i := walInstance.FirstSeg; i < watermark; i++ {
							deletePath := walInstance.GetSegmentFilename(i)
							err := os.Remove(filepath.Join(walDir, deletePath))
							if err != nil {
								fmt.Printf("Greška pri brisanju WAL segmenata")
							}
						}
						walInstance.FirstSeg = watermark
						fmt.Println("Prevođenje sadržaja Memtable u SSTable")
						sstrecords := memtable.ConvertMemToSST(&memtableInstances[mtIndex])

						_, newSSTdir, err := sstable.CreateSSTable(sstrecords, sstableDir, cfg.SummaryStep, bm, cfg.BlockSize, 0, cfg.SSTableSingleFile, cfg.SSTableCompression, dict, dictPath)
						if err != nil {
							fmt.Printf("Greška pri kreiranju SSTable: %v\n", err)
						}
						lsm[0] = append(lsm[0], newSSTdir)
						// Funkcija za proveru i izvršenje kompakcija
						switch cfg.CompactionAlgorithm {
						case "SizeTiered":
							sstable.SizeTieredCompaction(bm, &lsm, sstableDir, cfg.MaxCountInLevel,
								cfg.BlockSize, cfg.SummaryStep, cfg.SSTableSingleFile, cfg.SSTableCompression, dict, dictPath)
						case "Leveled":
							sstable.LeveledCompaction(bm, &lsm, sstableDir, cfg.MaxCountInLevel,
								cfg.BlockSize, cfg.SummaryStep, cfg.SSTableSingleFile, cfg.SSTableCompression, dict, dictPath)
						}
					}
				}
				// Ako je upis u WAL uspesan, dodaje se u Memtable
				fmt.Printf("Uspešno dodato u WAL i Memtable: [%s -> %s]\n", key, value)
			}

		// --------------------------------------------------------------------------------------------------------------------------
		// GET komanda
		// --------------------------------------------------------------------------------------------------------------------------

		// GET komanda ocekuje 1 argument: key
		case "GET":
			if strings.HasPrefix(parts[1], "__sys__") {
				fmt.Println("Zabranjena operacija nad internim kljucevima.")
				continue
			}

			if len(parts) != 2 {
				fmt.Println("Greska: GET zahteva <key>")
				continue
			}
			key := parts[1]

			found := false
			var value []byte

			// Pretrazi Memtable
			for i := 0; i < cfg.MemtableNum; i++ {
				value, found = memtableInstances[i].Get(key)
				if found {
					// fmt.Println("memtable")
					fmt.Printf("Vrednost za kljuc: [%s -> %s]\n", key, value)
					// Zapis u keš
					lru.UpdateCache(key, value)
					break
				}
			}
			if found {
				continue
			}

			// Pretrazi Cache
			value, found = lru.CheckCache(key)
			if found {
				// fmt.Println("cache")
				fmt.Printf("Vrednost za kljuc: [%s -> %s]\n", key, value)
				continue
			}

			// Pretrazi SSTable-ove
			maxLevel := byte(0)
			for level := range lsm {
				if level > maxLevel {
					maxLevel = level
				}
			}

		Loop:
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
						// fmt.Println("sstable")
						fmt.Printf("Vrednost za kljuc: [%s -> %s]\n", record.Key, record.Value)
						break Loop
					}
				}
			}

			if !found {
				fmt.Printf("Nije pronadjena vrednost za kljuc: [%s]\n", key)
			}

		// --------------------------------------------------------------------------------------------------------------------------
		// DELETE komanda
		// --------------------------------------------------------------------------------------------------------------------------

		// DELETE komanda ocekuje 1 argument: key
		case "DELETE":
			if strings.HasPrefix(parts[1], "__sys__") {
				fmt.Println("Zabranjena operacija nad internim kljucevima.")
				continue
			}

			if len(parts) != 2 {
				fmt.Println("Greska: DELETE zahteva <key>")
				continue
			}
			// Brisanje iz keša
			lru.DeleteFromCache(parts[1])

			// Konverzija kljuca u []byte, tombstone je true
			key := []byte(parts[1])

			var value []byte
			var found bool
			delIndex := 0
			for delIndex < cfg.MemtableNum {
				value, found = memtableInstances[delIndex].Get(parts[1])
				if found {
					break
				} else {
					delIndex++
				}
			}
			tombstone := true

			// Izbrisi iz WAL-a i Memtable-a
			if found {
				_, err = walInstance.AppendRecord(tombstone, key, value)
				if err != nil {
					fmt.Printf("Greska prilikom brisanja iz WAL-a: [%s -> %s]\n", key, value)
				} else {
					err := memtableInstances[delIndex].Delete(parts[1])
					if err != nil {
						fmt.Printf("Greska prilikom brisanja iz Memtable-a: %v\n", err)
					} else {
						fmt.Printf("Uspesno izbrisano iz WAL-a Memtable-a: [%s -> %s]\n", key, value)
					}
				}
			} else {
				fmt.Printf("Kljuc '%s' nije pronadjen.\n", key)
			}

		// --------------------------------------------------------------------------------------------------------------------------
		// VALIDATE komanda
		// --------------------------------------------------------------------------------------------------------------------------

		case "VALIDATE":
			if len(parts) != 1 {
				fmt.Println("Greska: VALIDATE ne zahteva dodatne parametre")
				continue
			}
			fmt.Println("Unesite broj jedne od datih SSTabela za validaciju: ")
			tableIndex := 1
			for _, level := range lsm {
				for _, tableDir := range level {
					fmt.Println(tableIndex, ") ", tableDir)
					tableIndex++
				}
			}

			if !scanner.Scan() {
				continue
			}

			tableNum, err := strconv.Atoi(strings.TrimSpace(scanner.Text()))
			if err != nil || tableNum >= tableIndex {
				fmt.Println("Neispravan unos!")
			}

			findTable := 1
			for _, level := range lsm {
				for _, tableDir := range level {
					if findTable == tableNum {
						sst, err := sstable.ReadTableFromDir(tableDir)
						if err != nil {
							fmt.Println("Greška u čitanju foldera!")
						}
						correct, err := sstable.ValidateMerkleTree(bm, sst, cfg.BlockSize)
						if correct {
							fmt.Println("Nisu pronađene greške u datoj SSTabeli!")
						} else {
							fmt.Println(err.Error())
						}
					}
					findTable++
				}
			}

		// ========== PROBABILISTIC ==========

		case "BLOOM_CREATE":
			if len(parts) != 4 {
				fmt.Println("Usage: BLOOM_CREATE <name> <expectedElements> <falsePositiveRate>")
				continue
			}
			name := parts[1]
			expected, _ := strconv.Atoi(parts[2])
			fpRate, _ := strconv.ParseFloat(parts[3], 64)
			bf := probabilistic.CreateBF(expected, fpRate)
			probabilistic.SaveBloom(name, &bf, memtableInstances[0])
			fmt.Println("Bloom filter kreiran.")

		case "BLOOM_ADD":
			if len(parts) != 3 {
				fmt.Println("Usage: BLOOM_ADD <name> <element>")
				continue
			}
			name := parts[1]
			elem := parts[2]
			bf, err := probabilistic.LoadBloom(name, memtableInstances[0])
			if err != nil {
				fmt.Println("Greska:", err)
				continue
			}
			bf.AddElement(elem)
			probabilistic.SaveBloom(name, bf, memtableInstances[0])
			fmt.Println("Element dodat u Bloom filter.")

		case "BLOOM_CHECK":
			if len(parts) != 3 {
				fmt.Println("Usage: BLOOM_CHECK <name> <element>")
				continue
			}
			name := parts[1]
			elem := parts[2]
			bf, err := probabilistic.LoadBloom(name, memtableInstances[0])
			if err != nil {
				fmt.Println("Greska:", err)
				continue
			}
			if bf.IsAdded(elem) {
				fmt.Println("Element je mozda prisutan.")
			} else {
				fmt.Println("Element sigurno nije prisutan.")
			}

		case "CMS_CREATE":
			if len(parts) != 4 {
				fmt.Println("Usage: CMS_CREATE <name> <epsilon> <delta>")
				continue
			}
			name := parts[1]
			epsilon, _ := strconv.ParseFloat(parts[2], 64)
			delta, _ := strconv.ParseFloat(parts[3], 64)
			cms := probabilistic.CreateCountMinSketch(epsilon, delta)
			probabilistic.SaveCMS(name, &cms, memtableInstances[0])
			fmt.Println("CMS kreiran.")

		case "CMS_ADD":
			if len(parts) != 3 {
				fmt.Println("Usage: CMS_ADD <name> <event>")
				continue
			}
			name := parts[1]
			event := parts[2]
			cms, err := probabilistic.LoadCMS(name, memtableInstances[0])
			if err != nil {
				fmt.Println("Greska:", err)
				continue
			}
			cms.Add(event)
			probabilistic.SaveCMS(name, cms, memtableInstances[0])
			fmt.Println("Dogadjaj dodat u CMS.")

		case "CMS_COUNT":
			if len(parts) != 3 {
				fmt.Println("Usage: CMS_COUNT <name> <event>")
				continue
			}
			name := parts[1]
			event := parts[2]
			cms, err := probabilistic.LoadCMS(name, memtableInstances[0])
			if err != nil {
				fmt.Println("Greska:", err)
				continue
			}
			count := cms.FindCount(event)
			fmt.Printf("Broj ponavljanja za %s: %d\n", event, count)

		case "HLL_CREATE":
			if len(parts) != 3 {
				fmt.Println("Usage: HLL_CREATE <name> <precision>")
				continue
			}
			name := parts[1]
			precision, _ := strconv.Atoi(parts[2])
			hll := probabilistic.CreateHLL(uint8(precision))
			probabilistic.SaveHLL(name, &hll, memtableInstances[0])
			fmt.Println("HLL kreiran.")

		case "HLL_ADD":
			if len(parts) != 3 {
				fmt.Println("Usage: HLL_ADD <name> <element>")
				continue
			}
			name := parts[1]
			elem := parts[2]
			hll, err := probabilistic.LoadHLL(name, memtableInstances[0])
			if err != nil {
				fmt.Println("Greska:", err)
				continue
			}
			hll.Add(elem)
			probabilistic.SaveHLL(name, hll, memtableInstances[0])
			fmt.Println("Element dodat u HLL.")

		case "HLL_COUNT":
			if len(parts) != 2 {
				fmt.Println("Usage: HLL_COUNT <name>")
				continue
			}
			name := parts[1]
			hll, err := probabilistic.LoadHLL(name, memtableInstances[0])
			if err != nil {
				fmt.Println("Greska:", err)
				continue
			}
			fmt.Println("Kardinalitet:", int(hll.Estimate()))

		case "SIMHASH_ADD":
			if len(parts) != 3 {
				fmt.Println("Usage: SIMHASH_ADD <name> <text>")
				continue
			}
			name := parts[1]
			text := parts[2]
			weights := probabilistic.GetWordWeights(text)
			sim := probabilistic.ComputeSimhash(weights)
			probabilistic.SaveSimhash(name, sim, memtableInstances[0])
			fmt.Println("Fingerprint sacuvan.")

		case "SIMHASH_DIST":
			if len(parts) != 3 {
				fmt.Println("Usage: SIMHASH_DIST <name1> <name2>")
				continue
			}
			s1, err1 := probabilistic.LoadSimhash(parts[1], memtableInstances[0])
			s2, err2 := probabilistic.LoadSimhash(parts[2], memtableInstances[0])
			if err1 != nil || err2 != nil {
				fmt.Println("Greska pri ucitavanju fingerprinta")
				continue
			}
			fmt.Printf("Hamming distanca: %d\n", probabilistic.HammingDistance(s1, s2))

		// --------------------------------------------------------------------------------------------------------------------------
		// PREFIX_SCAN komanda
		// --------------------------------------------------------------------------------------------------------------------------

		case "PREFIX_SCAN":
			if len(parts) != 4 {
				fmt.Println("Greska: PREFIX_SCAN zahteva <prefix> <pageNumber> <pageSize>")
				continue
			}

			prefix := parts[1]
			minKey := prefix
			maxKey := prefix + "\xff"
			pageNum, err1 := strconv.Atoi(parts[2])
			pageSize, err2 := strconv.Atoi(parts[3])

			if err1 != nil || err2 != nil || pageNum < 1 || pageSize < 1 {
				fmt.Println("Nevalidan broj ili velicina stranica.")
				continue
			}

			// Napravi cursore za sve memtabele
			cursors := make([]cursor.Cursor, 0, len(memtableInstances))
			for _, mt := range memtableInstances {
				cursors = append(cursors, mt.NewCursor())
			}

			// Napravi kursore za sve SSTabele
			sstableCursors := make([]cursor.Cursor, 0)
			for _, level := range lsm {
				for _, path := range level {
					sst, err := sstable.ReadTableFromDir(path)
					if err != nil {
						fmt.Printf("Greška prilikom čitanja SSTable...")
					}
					sum, err := sstable.ReadSummaryFromTable(sst, bm, cfg.BlockSize)
					if err != nil {
						fmt.Printf("Greška prilikom čitanja SSTable...")
					}
					if string(sum.MaxKey) < minKey || string(sum.MinKey) > maxKey {
						continue
					}
					var newOffset int
					for _, entry := range sum.Entries {
						if string(entry.Key) > minKey {
							break
						}
						newOffset = int(entry.Offset)
					}
					newCursor, err := sstable.NewCursor(bm, sst, minKey, maxKey, newOffset, cfg.BlockSize, cfg.SSTableCompression, dict)
					if err != nil {
						fmt.Printf("Greška prilikom formiranja kursora...")
					}
					sstableCursors = append(sstableCursors, &newCursor)
				}
			}

			cursors = append(cursors, sstableCursors...)

			// Napravi jedan multi cursor kao wrapper svih cursora
			mc := cursor.NewMultiCursor(minKey, maxKey, cursors...)
			defer mc.Close()

			// Prikupi sve zapise
			records := make(map[string]struct {
				value []byte
				ts    [16]byte
			})

			for mc.Next() {
				key := mc.Key()
				if key == "" {
					continue
				}

				// Preskoci ako je izbrisano
				if mc.Tombstone() {
					delete(records, key)
					continue
				}

				// Sacuvaj samo najnoviju verziju
				currTS := mc.Timestamp()
				if existing, ok := records[key]; ok {
					if bytes.Compare(currTS[:], existing.ts[:]) > 0 {
						// Novi zapis ima veci timestamp, azuriraj
						records[key] = struct {
							value []byte
							ts    [16]byte
						}{
							value: mc.Value(),
							ts:    currTS,
						}
					}
				} else {
					// Ne postoji u records, samo dodaj
					records[key] = struct {
						value []byte
						ts    [16]byte
					}{
						value: mc.Value(),
						ts:    currTS,
					}
				}
			}

			// Sortiraj kljuceve
			sortedKeys := make([]string, 0, len(records))
			for k := range records {
				if k >= minKey && k <= maxKey {
					sortedKeys = append(sortedKeys, k)
				}
			}
			sort.Strings(sortedKeys)

			// Paginacija
			total := len(sortedKeys)
			start := (pageNum - 1) * pageSize
			if start >= total {
				fmt.Printf("Nema zapisa (stranica %d od %d)\n", pageNum, (total+pageSize-1)/pageSize)
				continue
			}
			end := start + pageSize
			if end > total {
				end = total
			}

			// Prikazi rezultate
			fmt.Printf("Stranica %d (Kljucevi %d-%d od %d):\n", pageNum, start+1, end, total)
			for _, key := range sortedKeys[start:end] {
				fmt.Printf("Vrednost za kljuc: [%s -> %s]\n", key, records[key].value)
			}

			// Zatvori multicursor
			mc.Close()

		// --------------------------------------------------------------------------------------------------------------------------
		// RANGE_SCAN komanda
		// --------------------------------------------------------------------------------------------------------------------------

		case "RANGE_SCAN":
			if len(parts) != 5 {
				fmt.Println("Usage: RANGE_SCAN <minKey> <maxKey> <pageNumber> <pageSize>")
				continue
			}

			minKey := parts[1]
			maxKey := parts[2]
			pageNum, err1 := strconv.Atoi(parts[3])
			pageSize, err2 := strconv.Atoi(parts[4])

			if err1 != nil || err2 != nil || pageNum < 1 || pageSize < 1 {
				fmt.Println("Nevalidan broj ili velicina stranica.")
				continue
			}

			// Napravi cursore za sve memtabele
			cursors := make([]cursor.Cursor, 0, len(memtableInstances))
			for _, mt := range memtableInstances {
				cursors = append(cursors, mt.NewCursor())
			}

			// Napravi kursore za sve SSTabele
			sstableCursors := make([]cursor.Cursor, 0)
			for _, level := range lsm {
				for _, path := range level {
					sst, err := sstable.ReadTableFromDir(path)
					if err != nil {
						fmt.Printf("Greška prilikom čitanja SSTable...")
					}
					sum, err := sstable.ReadSummaryFromTable(sst, bm, cfg.BlockSize)
					if err != nil {
						fmt.Printf("Greška prilikom čitanja SSTable...")
					}
					if string(sum.MaxKey) < minKey || string(sum.MinKey) > maxKey {
						continue
					}
					var newOffset int
					for _, entry := range sum.Entries {
						if string(entry.Key) > minKey {
							break
						}
						newOffset = int(entry.Offset)
					}
					newCursor, err := sstable.NewCursor(bm, sst, minKey, maxKey, newOffset, cfg.BlockSize, cfg.SSTableCompression, dict)
					if err != nil {
						fmt.Printf("Greška prilikom formiranja kursora...")
					}
					sstableCursors = append(sstableCursors, &newCursor)
				}
			}

			cursors = append(cursors, sstableCursors...)

			// Napravi jedan multi cursor kao wrapper svih cursora
			mc := cursor.NewMultiCursor(minKey, maxKey, cursors...)
			defer mc.Close()

			// Prikupi sve zapise
			records := make(map[string]struct {
				value []byte
				ts    [16]byte
			})

			for mc.Next() {
				key := mc.Key()
				if key == "" {
					continue
				}

				// Preskoci ako je izbrisano
				if mc.Tombstone() {
					delete(records, key)
					continue
				}

				// Sacuvaj samo najnoviju verziju
				currTS := mc.Timestamp()
				if existing, ok := records[key]; ok {
					if bytes.Compare(currTS[:], existing.ts[:]) > 0 {
						// Novi zapis ima veci timestamp, azuriraj
						records[key] = struct {
							value []byte
							ts    [16]byte
						}{
							value: mc.Value(),
							ts:    currTS,
						}
					}
				} else {
					// Ne postoji u records, samo dodaj
					records[key] = struct {
						value []byte
						ts    [16]byte
					}{
						value: mc.Value(),
						ts:    currTS,
					}
				}
			}

			// Sortiraj kljuceve
			sortedKeys := make([]string, 0, len(records))
			for k := range records {
				if k >= minKey && k <= maxKey {
					sortedKeys = append(sortedKeys, k)
				}
			}
			sort.Strings(sortedKeys)

			// Paginacija
			total := len(sortedKeys)
			start := (pageNum - 1) * pageSize
			if start >= total {
				fmt.Printf("Nema zapisa (stranica %d od %d)\n", pageNum, (total+pageSize-1)/pageSize)
				continue
			}
			end := start + pageSize
			if end > total {
				end = total
			}

			// Prikazi rezultate
			fmt.Printf("Stranica %d (Kljucevi %d-%d od %d):\n", pageNum, start+1, end, total)
			for _, key := range sortedKeys[start:end] {
				fmt.Printf("Vrednost za kljuc: [%s -> %s]\n", key, records[key].value)
			}

			// Zatvori multicursor
			mc.Close()

		// --------------------------------------------------------------------------------------------------------------------------
		// PREFIX_ITERATE komanda
		// --------------------------------------------------------------------------------------------------------------------------

		case "PREFIX_ITERATE":
			if len(parts) != 2 {
				fmt.Println("Usage: PREFIX_ITERATE <prefix>")
				continue
			}

			prefix := parts[1]
			minKey := prefix
			maxKey := prefix + "\xff"

			// Napravi cursore za sve memtabele
			cursors := make([]cursor.Cursor, 0, len(memtableInstances))
			for _, mt := range memtableInstances {
				cursors = append(cursors, mt.NewCursor())
			}

			// Napravi kursore za sve SSTabele
			sstableCursors := make([]cursor.Cursor, 0)
			for _, level := range lsm {
				for _, path := range level {
					sst, err := sstable.ReadTableFromDir(path)
					if err != nil {
						fmt.Printf("Greška prilikom čitanja SSTable...")
					}
					sum, err := sstable.ReadSummaryFromTable(sst, bm, cfg.BlockSize)
					if err != nil {
						fmt.Printf("Greška prilikom čitanja SSTable...")
					}
					if string(sum.MaxKey) < minKey || string(sum.MinKey) > maxKey {
						continue
					}
					var newOffset int
					for _, entry := range sum.Entries {
						if string(entry.Key) > minKey {
							break
						}
						newOffset = int(entry.Offset)
					}
					newCursor, err := sstable.NewCursor(bm, sst, minKey, maxKey, newOffset, cfg.BlockSize, cfg.SSTableCompression, dict)
					if err != nil {
						fmt.Printf("Greška prilikom formiranja kursora...")
					}
					sstableCursors = append(sstableCursors, &newCursor)
				}
			}

			cursors = append(cursors, sstableCursors...)

			// Napravi jedan multi cursor kao wrapper svih cursora
			mc := cursor.NewMultiCursor(minKey, maxKey, cursors...)

			// Iterate petlja
		outer_prefix:
			for {
				mc.Next()
				key := mc.Key()

				if key == "" {
					break
				}

				// Preskoci izbrisane kljuceve
				if mc.Tombstone() {
					continue
				}

				fmt.Printf("Vrednost za kljuc: [%s -> %s]\n", key, mc.Value())
				fmt.Printf("\nNEXT/STOP\n> ")

				// Citanje linije iz inputa
				if !scanner.Scan() {
					break
				}

				// Podijeli input na dijelove
				input := strings.TrimSpace(scanner.Text())
				if len(input) == 0 {
					continue
				}

				switch strings.ToUpper(input) {
				case "STOP":
					// Izadji iz petlje
					break outer_prefix
				case "NEXT":
					// Predji na sledeci element
				default:
					fmt.Println("Unknown command. Use NEXT or STOP")
				}
			}

			// Zatvori multicursor
			mc.Close()

			// --------------------------------------------------------------------------------------------------------------------------
			// RANGE_ITERATE komanda
			// --------------------------------------------------------------------------------------------------------------------------

		case "RANGE_ITERATE":
			if len(parts) != 3 {
				fmt.Println("Usage: RANGE_ITERATE <minKey> <maxKey>")
				continue
			}

			minKey := parts[1]
			maxKey := parts[2]

			// Napravi cursore za sve memtabele
			cursors := make([]cursor.Cursor, 0, len(memtableInstances))
			for _, mt := range memtableInstances {
				cursors = append(cursors, mt.NewCursor())
			}

			// Napravi kursore za sve SSTabele
			sstableCursors := make([]cursor.Cursor, 0)
			for _, level := range lsm {
				for _, path := range level {
					sst, err := sstable.ReadTableFromDir(path)
					if err != nil {
						fmt.Printf("Greška prilikom čitanja SSTable...")
					}
					sum, err := sstable.ReadSummaryFromTable(sst, bm, cfg.BlockSize)
					if err != nil {
						fmt.Printf("Greška prilikom čitanja SSTable...")
					}
					if string(sum.MaxKey) < minKey || string(sum.MinKey) > maxKey {
						continue
					}
					var newOffset int
					for _, entry := range sum.Entries {
						if string(entry.Key) > minKey {
							break
						}
						newOffset = int(entry.Offset)
					}
					newCursor, err := sstable.NewCursor(bm, sst, minKey, maxKey, newOffset, cfg.BlockSize, cfg.SSTableCompression, dict)
					if err != nil {
						fmt.Printf("Greška prilikom formiranja kursora...")
					}
					sstableCursors = append(sstableCursors, &newCursor)
				}
			}

			cursors = append(cursors, sstableCursors...)

			// Napravi jedan multi cursor kao wrapper svih cursora
			mc := cursor.NewMultiCursor(minKey, maxKey, cursors...)

			// Iterate petlja
		outer_range:
			for {
				mc.Next()
				key := mc.Key()

				if key == "" {
					break
				}

				// Preskoci izbrisane kljuceve
				if mc.Tombstone() {
					continue
				}

				fmt.Printf("Vrednost za kljuc: [%s -> %s]\n", key, mc.Value())
				fmt.Printf("\nNEXT/STOP\n> ")

				// Citanje linije iz inputa
				if !scanner.Scan() {
					break
				}

				// Podijeli input na dijelove
				input := strings.TrimSpace(scanner.Text())
				if len(input) == 0 {
					continue
				}

				switch strings.ToUpper(input) {
				case "STOP":
					// Izadji iz petlje
					break outer_range
				case "NEXT":
					// Predji na sledeci element
				default:
					fmt.Println("Unknown command. Use NEXT or STOP")
				}
			}

			// Zatvori multicursor
			mc.Close()

		// --------------------------------------------------------------------------------------------------------------------------
		// HELP i EXIT komanda
		// --------------------------------------------------------------------------------------------------------------------------

		case "HELP":
			fmt.Println("Podrzane komande:")
			fmt.Println(" - PUT <key> <value>     - Dodaje ili azurira kljuc i vrednost.")
			fmt.Println(" - GET <key>             - Prikazuje vrednost za dati kljuc.")
			fmt.Println(" - DELETE <key>          - Brise podatke za dati kljuc.")
			fmt.Println(" - EXIT                  - Izlazi iz aplikacije.")

		case "EXIT":
			walInstance.WriteOnExit()

			err := dict.ForceSaveToFile(dictPath, bm, cfg.BlockSize)
			if err != nil {
				fmt.Printf("Greška pri upisu rečnika: %v\n", err)
			}

			fmt.Println("Dovidjenja!")
			return

		default:
			fmt.Println("Nepoznata komanda. Unesite 'help' za listu dostupnih komandi.")
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Greska pri citanju unosa:", err)
	}
}
