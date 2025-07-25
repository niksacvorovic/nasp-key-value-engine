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

	"projekat/utils"
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

	case "BTree":
		for i := 0; i < cfg.MemtableNum; i++ {
			memtableInstances[i] = containers.NewBTreeMemtable(cfg.MaxMemtableSize, cfg.BTreeDegree)
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

	fmt.Println("Dobrodošli u Key-Value Engine!")
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
		parts, err := utils.ParseArgs(input)
		if err != nil {
			fmt.Println("Greška pri parsiranju:", err)
			continue
		}

		if len(parts) == 0 {
			continue
		}

		// Parsiranje komande
		command := strings.ToUpper(parts[0])

		// Kontrola pristupa
		// Token bucket će se uvek čuvati u prvoj slobodnoj memtabeli pri pokretanju
		if command == "GET" || command == "PUT" || command == "DELETE" {
			bucket, _, ok := memtableInstances[tokenIndex].Get("__sys__TOKEN_BUCKET")
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
				fmt.Println("Prekoračen broj tokena! Molimo sačekajte...")
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
				fmt.Println("Zabranjena operacija nad internim ključevima.")
				continue
			}

			// Proverava da li PUT komanda ima tačno 2 argumenta: key i value
			if len(parts) != 3 {
				fmt.Println("Greška: PUT zahteva <ključ> <vrednost>")
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
				fmt.Printf("Greška pri pisanju u WAL: %v\n", err)
			} else {
				sstrecords := utils.WriteToMemory(ts, tombstone, parts[1], value, bm, &memtableInstances, &mtIndex, walInstance, cfg.MemtableNum)

				if sstrecords != nil {
					err := utils.WriteToDisk(sstrecords, sstableDir, bm, &lsm, cfg, dict, dictPath)
					if err != nil {
						fmt.Printf("Greška pri kreiranju SSTable: %v\n", err)
					}
				}

				// Ako je upis u WAL uspesan, dodaje se u Memtable
				fmt.Printf("Uspešno dodato: [%s -> %s]\n", utils.MaybeQuote(string(key)), utils.MaybeQuote(string(value)))
			}

		// --------------------------------------------------------------------------------------------------------------------------
		// GET komanda
		// --------------------------------------------------------------------------------------------------------------------------

		// GET komanda ocekuje 1 argument: key
		case "GET":
			if strings.HasPrefix(parts[1], "__sys__") {
				fmt.Println("Zabranjena operacija nad internim ključevima.")
				continue
			}

			if len(parts) != 2 {
				fmt.Println("Greška: GET zahteva <ključ>")
				continue
			}
			key := parts[1]

			var found bool
			var deleted bool
			var value []byte

			// Pretrazi Memtable
			for i := 0; i < cfg.MemtableNum; i++ {
				value, deleted, found = memtableInstances[i].Get(key)
				if found && !deleted {
					// fmt.Println("memtable")
					fmt.Printf("Pronađena vrednost: [%s -> %s]\n", utils.MaybeQuote(key), utils.MaybeQuote(string(value)))
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
				fmt.Printf("Pronađena vrednost: [%s -> %s]\n", utils.MaybeQuote(key), utils.MaybeQuote(string(value)))
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
						fmt.Printf("Pronađena vrednost: [%s -> %s]\n", utils.MaybeQuote(string(record.Key)), utils.MaybeQuote(string(record.Value)))
						break Loop
					}
				}
			}

			if !found {
				fmt.Printf("Nije pronadjena vrednost za kljuc: [%s]\n", utils.MaybeQuote(key))
			}

		// --------------------------------------------------------------------------------------------------------------------------
		// DELETE komanda
		// --------------------------------------------------------------------------------------------------------------------------

		// DELETE komanda ocekuje 1 argument: key
		case "DELETE":
			if strings.HasPrefix(parts[1], "__sys__") {
				fmt.Println("Zabranjena operacija nad internim ključevima.")
				continue
			}

			if len(parts) != 2 {
				fmt.Println("Greška: DELETE zahteva <ključ>")
				continue
			}
			// Brisanje iz keša
			lru.DeleteFromCache(parts[1])

			// Konverzija kljuca u []byte, tombstone je true
			key := []byte(parts[1])

			var value []byte
			var deleted bool
			var found bool
			delIndex := 0
			for delIndex < cfg.MemtableNum {
				value, deleted, found = memtableInstances[delIndex].Get(parts[1])
				if found {
					break
				} else {
					delIndex++
				}
			}
			tombstone := true

			// Izbrisi iz WAL-a i Memtable-a
			ts, err := walInstance.AppendRecord(tombstone, key, value)
			if err != nil {
				fmt.Printf("Greška prilikom brisanja iz WAL-a: [%s -> %s]\n", utils.MaybeQuote(string(key)), utils.MaybeQuote(string(value)))
			}
			if found && !deleted {
				deleted := memtableInstances[delIndex].Delete(parts[1])
				if deleted {
					fmt.Printf("Uspešno izbrisano iz Memtable-a: [%s -> %s]\n", utils.MaybeQuote(string(key)), utils.MaybeQuote(string(value)))
				}
			} else if found && deleted {
				fmt.Printf("Ključ [%s] je obrisan u Memtable\n", utils.MaybeQuote(string(key)))
			} else {
				// Zapis je potencijalno u SSTable - zapisujemo njegovo brisanje
				sstrecords := utils.WriteToMemory(ts, tombstone, parts[1], []byte{}, bm, &memtableInstances, &mtIndex, walInstance, cfg.MemtableNum)
				if sstrecords != nil {
					err := utils.WriteToDisk(sstrecords, sstableDir, bm, &lsm, cfg, dict, dictPath)
					if err != nil {
						fmt.Printf("Greška pri kreiranju SSTable: %v\n", err)
					}
				}
				fmt.Printf("Ključ nije pronađen: [%s]\n", utils.MaybeQuote(string(key)))
				fmt.Printf("Brisanje evidentirano u sistemu: [%s -> %s]\n", utils.MaybeQuote(string(key)), utils.MaybeQuote(string(value)))
			}

		// --------------------------------------------------------------------------------------------------------------------------
		// VALIDATE komanda
		// --------------------------------------------------------------------------------------------------------------------------

		case "VALIDATE":
			if len(parts) != 1 {
				fmt.Println("Greška: VALIDATE ne zahteva argumente")
				continue
			}
			tableIndex := 1
			for _, level := range lsm {
				for _, tableDir := range level {
					fmt.Println(tableIndex, ") ", tableDir)
					tableIndex++
				}
			}
			fmt.Print("Izaberite SSTabelu za validaciju: ")

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
						indices, err := sstable.ValidateMerkleTree(bm, sst, cfg.BlockSize)
						if err != nil {
							fmt.Println("Greška u poređenju tabele!")
						}
						if indices == nil {
							fmt.Println("SSTabela je validna, nema grešaka.")
						} else {
							fmt.Println("Promene detektovane - neispravni blokovi: ", indices)
						}
					}
					findTable++
				}
			}

		// ================================ PROBABILISTIC ================================

		// -----------------------------------
		// BLOOM FILTER
		// -----------------------------------

		case "BLOOM_CREATE":
			if len(parts) != 4 {
				fmt.Println("Greska: BLOOM_CREATE zahteva <ime> <ocekivani_broj> <greska>")
				continue
			}

			name := parts[1]
			expected, err1 := strconv.Atoi(parts[2])
			fpRate, err2 := strconv.ParseFloat(parts[3], 64)
			if err1 != nil || err2 != nil {
				fmt.Println("Greska: nevalidni brojevi")
				continue
			}

			key := "__sys__prob__bf__" + name
			bf := probabilistic.CreateBF(expected, fpRate)
			value := bf.Serialize()

			// WAL
			ts, err := walInstance.AppendRecord(false, []byte(key), value)
			if err != nil {
				fmt.Println("Greska pri pisanju u WAL:", err)
				continue
			}

			// Memtable
			sstRecords := utils.WriteToMemory(ts, false, key, value, bm, &memtableInstances, &mtIndex, walInstance, cfg.MemtableNum)
			if sstRecords != nil {
				err := utils.WriteToDisk(sstRecords, sstableDir, bm, &lsm, cfg, dict, dictPath)
				if err != nil {
					fmt.Printf("Greska pri kreiranju SSTable: %v\n", err)
				}
			}

			fmt.Println("Bloom filter kreiran:", name)

		case "BLOOM_ADD":
			if len(parts) != 3 {
				fmt.Println("Greska: BLOOM_ADD zahteva <ime> <element>")
				continue
			}

			name := parts[1]
			elem := parts[2]
			key := "__sys__prob__bf__" + name

			// 1. pokusaj da prositas iz Memtable
			var data []byte
			var found bool
			var deleted bool
			for i := 0; i < cfg.MemtableNum; i++ {
				data, deleted, found = memtableInstances[i].Get(key)
				if found {
					break
				}
			}

			// 2. ako nije u memtable, idi u SSTable
			if !found || deleted {
				maxLevel := byte(0)
				for level := range lsm {
					if level > maxLevel {
						maxLevel = level
					}
				}

			LoopSST:
				for level := byte(0); level <= maxLevel; level++ {
					sstableDirs, exists := lsm[level]
					if !exists {
						continue
					}
					for _, dir := range sstableDirs {
						table, err := sstable.ReadTableFromDir(dir)
						if err != nil {
							continue
						}
						record, ok := sstable.SearchSSTable(table, key, cfg, bm, dict)
						if ok {
							data = record.Value
							break LoopSST
						}
					}
				}
			}

			if data == nil {
				fmt.Println("Greska: Bloom filter nije pronadjn.")
				continue
			}

			bf := &probabilistic.BloomFilter{}
			bf.Deserialize(data)
			bf.AddElement(elem)
			value := bf.Serialize()

			// WAL
			ts, err := walInstance.AppendRecord(false, []byte(key), value)
			if err != nil {
				fmt.Println("Greska pri pisanju u WAL:", err)
				continue
			}

			// Memtable
			sstRecords := utils.WriteToMemory(ts, false, key, value, bm, &memtableInstances, &mtIndex, walInstance, cfg.MemtableNum)
			if sstRecords != nil {
				err := utils.WriteToDisk(sstRecords, sstableDir, bm, &lsm, cfg, dict, dictPath)
				if err != nil {
					fmt.Printf("Greska pri kreiranju SSTable: %v\n", err)
				}
			}

			fmt.Println("Element dodat u Bloom filter.")

		case "BLOOM_CHECK":
			if len(parts) != 3 {
				fmt.Println("Greska: BLOOM_CHECK zahteva <ime> <element>")
				continue
			}

			name := parts[1]
			elem := parts[2]
			key := "__sys__prob__bf__" + name

			var data []byte
			var found bool
			var deleted bool
			for i := 0; i < cfg.MemtableNum; i++ {
				data, deleted, found = memtableInstances[i].Get(key)
				if found {
					break
				}
			}

			if (!found || deleted) && !deleted {
				maxLevel := byte(0)
				for level := range lsm {
					if level > maxLevel {
						maxLevel = level
					}
				}

			LoopSST2:
				for level := byte(0); level <= maxLevel; level++ {
					sstableDirs, exists := lsm[level]
					if !exists {
						continue
					}
					for _, dir := range sstableDirs {
						table, err := sstable.ReadTableFromDir(dir)
						if err != nil {
							continue
						}
						record, ok := sstable.SearchSSTable(table, key, cfg, bm, dict)
						if ok {
							data = record.Value
							break LoopSST2
						}
					}
				}
			}

			if data == nil {
				fmt.Println("Bloom filter nije pronadjen.")
				continue
			}

			bf := &probabilistic.BloomFilter{}
			bf.Deserialize(data)
			if bf.IsAdded(elem) {
				fmt.Println("Element verovatno postoji.")
			} else {
				fmt.Println("Element sigurno ne postoji.")
			}

		case "BLOOM_DELETE":
			if len(parts) != 2 {
				fmt.Println("Greska: BLOOM_DELETE zahteva <ime>")
				continue
			}

			name := parts[1]
			key := "__sys__prob__bf__" + name

			ts, err := walInstance.AppendRecord(true, []byte(key), nil)
			if err != nil {
				fmt.Println("Greska prilikom upisa u WAL:", err)
				continue
			}

			sstRecords := utils.WriteToMemory(ts, true, key, nil, bm, &memtableInstances, &mtIndex, walInstance, cfg.MemtableNum)
			if sstRecords != nil {
				err := utils.WriteToDisk(sstRecords, sstableDir, bm, &lsm, cfg, dict, dictPath)
				if err != nil {
					fmt.Println("Greska pri pisanju SSTable:", err)
				}
			}

			fmt.Println("Bloom filter obrisan:", name)

		// --------------------------------------------------------------------------------------------------------------------------
		// PREFIX_SCAN komanda
		// --------------------------------------------------------------------------------------------------------------------------

		case "PREFIX_SCAN":
			if len(parts) != 4 {
				fmt.Println("Greška: PREFIX_SCAN zahteva <prefiks> <broj_strane> <veličina_strane>")
				continue
			}

			prefix := parts[1]
			minKey := prefix
			maxKey := prefix + "\xff"
			pageNum, err1 := strconv.Atoi(parts[2])
			pageSize, err2 := strconv.Atoi(parts[3])

			if err1 != nil || err2 != nil || pageNum < 1 || pageSize < 1 {
				fmt.Println("Nevalidan broj ili veličina stranica.")
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
						fmt.Printf("Greška prilikom čitanja SSTable.")
					}
					sum, err := sstable.ReadSummaryFromTable(sst, bm, cfg.BlockSize)
					if err != nil {
						fmt.Printf("Greška prilikom čitanja SSTable.")
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
						fmt.Printf("Greška prilikom formiranja kursora.")
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
			fmt.Printf("Strana %d (rezultati %d-%d od %d):\n", pageNum, start+1, end, total)
			for _, key := range sortedKeys[start:end] {
				fmt.Printf("- [%s -> %s]\n", utils.MaybeQuote(key), utils.MaybeQuote(string(records[key].value)))
			}

			// Zatvori multicursor
			mc.Close()

		// --------------------------------------------------------------------------------------------------------------------------
		// RANGE_SCAN komanda
		// --------------------------------------------------------------------------------------------------------------------------

		case "RANGE_SCAN":
			if len(parts) != 5 {
				fmt.Println("Greška: RANGE_SCAN zahteva <početni_ključ> <krajnji_ključ> <broj_strane> <veličina_strane>")
				continue
			}

			minKey := parts[1]
			maxKey := parts[2]
			pageNum, err1 := strconv.Atoi(parts[3])
			pageSize, err2 := strconv.Atoi(parts[4])

			if err1 != nil || err2 != nil || pageNum < 1 || pageSize < 1 {
				fmt.Println("Nevalidan broj ili veličina stranica.")
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
						fmt.Printf("Greška prilikom čitanja SSTable.")
					}
					sum, err := sstable.ReadSummaryFromTable(sst, bm, cfg.BlockSize)
					if err != nil {
						fmt.Printf("Greška prilikom čitanja SSTable.")
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
						fmt.Printf("Greška prilikom formiranja kursora.")
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
			fmt.Printf("Strana %d (rezultati %d-%d od %d):\n", pageNum, start+1, end, total)
			for _, key := range sortedKeys[start:end] {
				fmt.Printf("- [%s -> %s]\n", utils.MaybeQuote(key), utils.MaybeQuote(string(records[key].value)))
			}

			// Zatvori multicursor
			mc.Close()

		// --------------------------------------------------------------------------------------------------------------------------
		// PREFIX_ITERATE komanda
		// --------------------------------------------------------------------------------------------------------------------------

		case "PREFIX_ITERATE":
			if len(parts) != 2 {
				fmt.Println("Greška: PREFIX_ITERATE zahteva <prefiks>")
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
						fmt.Printf("Greška prilikom čitanja SSTable.")
					}
					sum, err := sstable.ReadSummaryFromTable(sst, bm, cfg.BlockSize)
					if err != nil {
						fmt.Printf("Greška prilikom čitanja SSTable.")
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
						fmt.Printf("Greška prilikom formiranja kursora.")
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

				fmt.Printf("- [%s -> %s]\n", utils.MaybeQuote(key), utils.MaybeQuote(string(mc.Value())))
				fmt.Print("Naredba (NEXT/STOP): ")

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
					fmt.Println("Nepoznata komanda. Upotrebite NEXT ili STOP")
				}
			}

			// Zatvori multicursor
			mc.Close()

			// --------------------------------------------------------------------------------------------------------------------------
			// RANGE_ITERATE komanda
			// --------------------------------------------------------------------------------------------------------------------------

		case "RANGE_ITERATE":
			if len(parts) != 3 {
				fmt.Println("Greška: RANGE_ITERATE zahteva <početni_ključ> <krajnji_ključ>")
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
						fmt.Printf("Greška prilikom čitanja SSTable.")
					}
					sum, err := sstable.ReadSummaryFromTable(sst, bm, cfg.BlockSize)
					if err != nil {
						fmt.Printf("Greška prilikom čitanja SSTable.")
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
						fmt.Printf("Greška prilikom formiranja kursora.")
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

				fmt.Printf("- [%s -> %s]\n", utils.MaybeQuote(key), utils.MaybeQuote(string(mc.Value())))
				fmt.Print("Naredba (NEXT/STOP): ")

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
					fmt.Println("Nepoznata komanda. Upotrebite NEXT ili STOP")
				}
			}

			// Zatvori multicursor
			mc.Close()

		// --------------------------------------------------------------------------------------------------------------------------
		// HELP i EXIT komanda
		// --------------------------------------------------------------------------------------------------------------------------

		case "HELP":
			fmt.Println("Dostupne komande:")
			fmt.Println("  PUT <ključ> <vrednost>        - Dodaje ili ažurira par")
			fmt.Println("  GET <ključ>                   - Prikazuje vrednost za ključ")
			fmt.Println("  DELETE <ključ>                - Briše vrednost za ključ")
			fmt.Println("  PREFIX_SCAN <prefiks> <str> <vel> - Pretraga po prefiksu (strana, veličina)")
			fmt.Println("  RANGE_SCAN <start> <kraj> <str> <vel> - Pretraga po opsegu (strana, veličina)")
			fmt.Println("  PREFIX_ITERATE <prefiks>      - Iterativna pretraga po prefiksu")
			fmt.Println("  RANGE_ITERATE <start> <kraj>  - Iterativna pretraga po opsegu")
			fmt.Println("  VALIDATE                      - Provera validnosti SSTabele")
			fmt.Println("")
			fmt.Println("Probabilističke strukture:")
			fmt.Println("  BLOOM_CREATE <naziv> <očekivani> <greška>  - Kreira Bloom filter")
			fmt.Println("  BLOOM_ADD <naziv> <element>     - Dodaje element u Bloom filter")
			fmt.Println("  BLOOM_CHECK <naziv> <element>   - Proverava element u Bloom filteru")
			fmt.Println("  CMS_CREATE <naziv> <epsilon> <delta> - Kreira Count-Min Sketch")
			fmt.Println("  CMS_ADD <naziv> <dogadjaj>      - Dodaje događaj u CMS")
			fmt.Println("  CMS_COUNT <naziv> <dogadjaj>    - Broji događaje u CMS")
			fmt.Println("  HLL_CREATE <naziv> <preciznost> - Kreira HyperLogLog")
			fmt.Println("  HLL_ADD <naziv> <element>       - Dodaje element u HLL")
			fmt.Println("  HLL_COUNT <naziv>               - Procenjuje kardinalitet u HLL")
			fmt.Println("  SIMHASH_ADD <naziv> <tekst>     - Kreira SimHash fingerprint")
			fmt.Println("  SIMHASH_DIST <naziv1> <naziv2>  - Računa Hamming distancu")
			fmt.Println("")
			fmt.Println("Ostale komande:")
			fmt.Println("  EXIT                      - Izlaz iz programa")
			fmt.Println("  HELP                      - Prikaz pomoći")
			fmt.Println("\nOpis parametara:")
			fmt.Println("  <str> - Broj stranice (počinje od 1)")
			fmt.Println("  <vel> - Veličina stranice (broj rezultata po stranici)")
			fmt.Println("  <očekivani> - Očekivani broj elemenata za Bloom filter")
			fmt.Println("  <greška> - Željena stopa lažno pozitivnih rezultata (0-1)")
			fmt.Println("  <epsilon> - Greška u proceni za CMS")
			fmt.Println("  <delta> - Verovatnoća greške za CMS")
			fmt.Println("  <preciznost> - Preciznost za HLL (4-16)")

		case "EXIT":
			walInstance.WriteOnExit()

			err := dict.ForceSaveToFile(dictPath, bm, cfg.BlockSize)
			if err != nil {
				fmt.Printf("Greška pri upisu rečnika: %v\n", err)
			}

			fmt.Println("Doviđenja!")
			return

		default:
			fmt.Println("Nepoznata komanda. Unesite 'help' za listu dostupnih komandi.")
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Greška pri čitanju unosa:", err)
	}
}
