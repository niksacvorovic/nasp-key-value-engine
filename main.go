package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"math"
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

	// Određivanje najstarijeg i najnovijeg indeksa i čitanje WAL segmenata redom
	if len(recordMap) != 0 {

		var minWalIndex uint32 = math.MaxUint32
		var maxWalIndex uint32 = 0

		for i := range recordMap {
			if i < minWalIndex {
				minWalIndex = i
			}
			if i > maxWalIndex {
				maxWalIndex = i
			}
		}
		// Dodavanje WAL zapisa u Memtabele
		for i := minWalIndex; i <= maxWalIndex; i++ {
			records, ok := recordMap[i]
			if !ok {
				continue
			}
			for _, rec := range records {
				// Dodavanje zapisa u memtable
				memtableInstances[mtIndex].Add(rec.Timestamp, rec.Tombstone, string(rec.Key), rec.Value)

				// Postavljanje watermarka za svaki Memtable
				memtableInstances[mtIndex].SetWatermark(i)

				// Provera da li je trenutni Memtable pun
				if memtableInstances[mtIndex].IsFull() {
					mtIndex = (mtIndex + 1) % cfg.MemtableNum
				}
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
		if utils.CommandsWithTokens[command] {
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

			var found bool = false
			var deleted bool
			var value []byte
			var record *sstable.Record

			// Pretrazi Memtable
			for i := 0; i < cfg.MemtableNum; i++ {
				value, deleted, found = memtableInstances[i].Get(key)
				if found && !deleted {
					fmt.Printf("Pronađena vrednost: [%s -> %s]\n", utils.MaybeQuote(key), utils.MaybeQuote(string(value)))
					break
				}
			}
			if found {
				continue
			}

			// Pretrazi Cache
			value, found = lru.CheckCache(key)
			if found {
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
			record = utils.ReadFromDisk(key, maxLevel, lsm, cfg, bm, dict)
			if record != nil {
				found = true
				lru.UpdateCache(string(record.Key), record.Value)
				fmt.Printf("Pronađena vrednost: [%s -> %s]\n", utils.MaybeQuote(string(record.Key)), utils.MaybeQuote(string(record.Value)))
			}
			if found {
				continue
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

			// 2. ako nije u memtable, idi u cache
			if !found {
				data, found = lru.CheckCache(key)
			}

			// 3. ako nije u cache, idi u SSTable
			if !found || deleted {
				maxLevel := byte(0)
				for level := range lsm {
					if level > maxLevel {
						maxLevel = level
					}
				}

				record := utils.ReadFromDisk(key, maxLevel, lsm, cfg, bm, dict)
				if record != nil {
					data = record.Value
					lru.UpdateCache(key, data)
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

			// Cache
			_, inCache := lru.CheckCache(key)
			if inCache {
				lru.UpdateCache(key, value)
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
			if !found {
				data, found = lru.CheckCache(key)
			}

			if (!found || deleted) && !deleted {
				maxLevel := byte(0)
				for level := range lsm {
					if level > maxLevel {
						maxLevel = level
					}
				}

				record := utils.ReadFromDisk(key, maxLevel, lsm, cfg, bm, dict)
				if record != nil {
					data = record.Value
					lru.UpdateCache(key, data)
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

			_, inCache := lru.CheckCache(key)
			if inCache {
				lru.DeleteFromCache(key)
			}

			fmt.Println("Bloom filter obrisan:", name)

		// -----------------------------------
		// COUNT-MIN SKETCH
		// -----------------------------------

		case "CMS_CREATE":
			if len(parts) != 4 {
				fmt.Println("Greska: CMS_CREATE zahteva <ime> <epsilon> <delta>")
				continue
			}

			name := parts[1]
			epsilon, err1 := strconv.ParseFloat(parts[2], 64)
			delta, err2 := strconv.ParseFloat(parts[3], 64)
			if err1 != nil || err2 != nil {
				fmt.Println("Greska: Nevalidni parametri")
				continue
			}

			key := "__sys__prob__cms__" + name
			cms := probabilistic.CreateCountMinSketch(epsilon, delta)
			value := cms.Serialize()

			ts, err := walInstance.AppendRecord(false, []byte(key), value)
			if err != nil {
				fmt.Println("Greska pri pisanju u WAL:", err)
				continue
			}

			sstRecords := utils.WriteToMemory(ts, false, key, value, bm, &memtableInstances, &mtIndex, walInstance, cfg.MemtableNum)
			if sstRecords != nil {
				err := utils.WriteToDisk(sstRecords, sstableDir, bm, &lsm, cfg, dict, dictPath)
				if err != nil {
					fmt.Println("Greska pri pisanju SSTable:", err)
				}
			}

			fmt.Println("Count-Min Sketch kreiran:", name)

		case "CMS_ADD":
			if len(parts) != 3 {
				fmt.Println("Greska: CMS_ADD zahteva <ime> <element>")
				continue
			}

			name := parts[1]
			elem := parts[2]
			key := "__sys__prob__cms__" + name

			var data []byte
			var found bool
			var deleted bool
			for i := 0; i < cfg.MemtableNum; i++ {
				data, deleted, found = memtableInstances[i].Get(key)
				if found {
					break
				}
			}

			if !found {
				data, found = lru.CheckCache(key)
			}

			if !found || deleted {
				maxLevel := byte(0)
				for level := range lsm {
					if level > maxLevel {
						maxLevel = level
					}
				}

				record := utils.ReadFromDisk(key, maxLevel, lsm, cfg, bm, dict)
				if record != nil {
					data = record.Value
					lru.UpdateCache(key, data)
				}
			}

			if data == nil {
				fmt.Println("Count-Min Sketch nije pronadjen.")
				continue
			}

			cms := &probabilistic.CountMinSketch{}
			err := cms.DeserializeFromBytes(data)
			if err != nil {
				fmt.Println("Greska pri deserijalizaciji CMS:", err)
				continue
			}
			cms.Add(elem)
			value := cms.Serialize()

			ts, err := walInstance.AppendRecord(false, []byte(key), value)
			if err != nil {
				fmt.Println("Greska pri pisanju u WAL:", err)
				continue
			}

			sstRecords := utils.WriteToMemory(ts, false, key, value, bm, &memtableInstances, &mtIndex, walInstance, cfg.MemtableNum)
			if sstRecords != nil {
				err := utils.WriteToDisk(sstRecords, sstableDir, bm, &lsm, cfg, dict, dictPath)
				if err != nil {
					fmt.Println("Greska pri pisnju SSTable:", err)
				}
			}

			_, inCache := lru.CheckCache(key)
			if inCache {
				lru.UpdateCache(key, value)
			}

			fmt.Println("Element dodat u CMS:", elem)

		case "CMS_COUNT":
			if len(parts) != 3 {
				fmt.Println("Greska: CMS_COUNT zahteva <ime> <element>")
				continue
			}

			name := parts[1]
			elem := parts[2]
			key := "__sys__prob__cms__" + name

			var data []byte
			var found bool
			var deleted bool
			for i := 0; i < cfg.MemtableNum; i++ {
				data, deleted, found = memtableInstances[i].Get(key)
				if found {
					break
				}
			}

			if !found {
				data, found = lru.CheckCache(key)
			}

			if !found || deleted {
				maxLevel := byte(0)
				for level := range lsm {
					if level > maxLevel {
						maxLevel = level
					}
				}

				record := utils.ReadFromDisk(key, maxLevel, lsm, cfg, bm, dict)
				if record != nil {
					data = record.Value
					lru.UpdateCache(key, data)
				}
			}

			if data == nil {
				fmt.Println("Count-Min Sketch nije pronadjen.")
				continue
			}

			cms := &probabilistic.CountMinSketch{}
			err := cms.DeserializeFromBytes(data)
			if err != nil {
				fmt.Println("Greska pri deserijalizaciji CMS:", err)
				continue
			}

			count := cms.FindCount(elem)
			fmt.Printf("Element '%s' se pojavljuje otprilike %d puta\n", elem, count)

		case "CMS_DELETE":
			if len(parts) != 2 {
				fmt.Println("Greska: CMS_DELETE zahteva <ime>")
				continue
			}

			name := parts[1]
			key := "__sys__prob__cms__" + name

			ts, err := walInstance.AppendRecord(true, []byte(key), nil)
			if err != nil {
				fmt.Println("Greska pri pisanju u WAL:", err)
				continue
			}
			sstRecords := utils.WriteToMemory(ts, true, key, nil, bm, &memtableInstances, &mtIndex, walInstance, cfg.MemtableNum)
			if sstRecords != nil {
				err := utils.WriteToDisk(sstRecords, sstableDir, bm, &lsm, cfg, dict, dictPath)
				if err != nil {
					fmt.Println("Greska pri pisanju SSTable:", err)
				}
			}
			_, inCache := lru.CheckCache(key)
			if inCache {
				lru.DeleteFromCache(key)
			}
			fmt.Println("Count-Min Sketch obrisan:", name)

		// -----------------------------------
		// HYPERLOGLOG
		// -----------------------------------

		case "HLL_CREATE":
			if len(parts) != 3 {
				fmt.Println("Greska: HLL_CREATE zahteva <ime> <precision>")
				continue
			}

			name := parts[1]
			precision, err := strconv.ParseUint(parts[2], 10, 8)
			if err != nil || precision < 4 || precision > 16 {
				fmt.Println("Greska: Precision mora biti broj između 4 i 16")
				continue
			}

			key := "__sys__prob__hll__" + name
			hll := probabilistic.CreateHLL(uint8(precision))
			value := hll.Serialize()

			ts, err := walInstance.AppendRecord(false, []byte(key), value)
			if err != nil {
				fmt.Println("Greska pri pisanju u WAL:", err)
				continue
			}

			sstRecords := utils.WriteToMemory(ts, false, key, value, bm, &memtableInstances, &mtIndex, walInstance, cfg.MemtableNum)
			if sstRecords != nil {
				err := utils.WriteToDisk(sstRecords, sstableDir, bm, &lsm, cfg, dict, dictPath)
				if err != nil {
					fmt.Println("Greska pri pisanju SSTable:", err)
				}
			}

			fmt.Println("HLL instanca kreirana:", name)

		case "HLL_ADD":
			if len(parts) != 3 {
				fmt.Println("Greska: HLL_ADD zahteva <ime> <element>")
				continue
			}

			name := parts[1]
			elem := parts[2]
			key := "__sys__prob__hll__" + name

			var data []byte
			var found, deleted bool

			for i := 0; i < cfg.MemtableNum; i++ {
				data, deleted, found = memtableInstances[i].Get(key)
				if found {
					break
				}
			}

			if !found {
				data, found = lru.CheckCache(key)
			}

			if !found || deleted {
				maxLevel := byte(0)
				for level := range lsm {
					if level > maxLevel {
						maxLevel = level
					}
				}

				record := utils.ReadFromDisk(key, maxLevel, lsm, cfg, bm, dict)
				if record != nil {
					data = record.Value
					lru.UpdateCache(key, data)
				}
			}

			if data == nil {
				fmt.Println("HLL nije pronadjen.")
				continue
			}

			hll := &probabilistic.HyperLogLog{}
			err := hll.Deserialize(data)
			if err != nil {
				fmt.Println("Greska pri deserijalizaciji HLL:", err)
				continue
			}

			hll.Add(elem)
			value := hll.Serialize()

			var ts [16]byte
			ts, err = walInstance.AppendRecord(false, []byte(key), value)
			if err != nil {
				fmt.Println("Greska pri pisanju u WAL:", err)
				continue
			}

			sstRecords := utils.WriteToMemory(ts, false, key, value, bm, &memtableInstances, &mtIndex, walInstance, cfg.MemtableNum)
			if sstRecords != nil {
				err := utils.WriteToDisk(sstRecords, sstableDir, bm, &lsm, cfg, dict, dictPath)
				if err != nil {
					fmt.Println("Greska pri pisanju SSTable:", err)
				}
			}

			_, inCache := lru.CheckCache(key)
			if inCache {
				lru.UpdateCache(key, value)
			}

			fmt.Println("Element dodat u HLL:", elem)

		case "HLL_COUNT":
			if len(parts) != 2 {
				fmt.Println("Greska: HLL_COUNT zahteva <ime>")
				continue
			}

			name := parts[1]
			key := "__sys__prob__hll__" + name

			var data []byte
			var found, deleted bool

			for i := 0; i < cfg.MemtableNum; i++ {
				data, deleted, found = memtableInstances[i].Get(key)
				if found {
					break
				}
			}

			if !found {
				data, found = lru.CheckCache(key)
			}

			if !found || deleted {
				maxLevel := byte(0)
				for level := range lsm {
					if level > maxLevel {
						maxLevel = level
					}
				}

				record := utils.ReadFromDisk(key, maxLevel, lsm, cfg, bm, dict)
				if record != nil {
					data = record.Value
					lru.UpdateCache(key, data)
				}
			}

			if data == nil {
				fmt.Println("HLL nije pronadjen.")
				continue
			}

			hll := &probabilistic.HyperLogLog{}
			err := hll.Deserialize(data)
			if err != nil {
				fmt.Println("Greska pri deserijalizaciji HLL:", err)
				continue
			}

			fmt.Printf("Procenjena kardinalnost: %.0f\n", hll.Estimate())

		case "HLL_DELETE":
			if len(parts) != 2 {
				fmt.Println("Greska: HLL_DELETE zahteva <ime>")
				continue
			}

			name := parts[1]
			key := "__sys__prob__hll__" + name

			ts, err := walInstance.AppendRecord(true, []byte(key), nil)
			if err != nil {
				fmt.Println("Greska pri pisanju u WAL:", err)
				continue
			}

			sstRecords := utils.WriteToMemory(ts, true, key, nil, bm, &memtableInstances, &mtIndex, walInstance, cfg.MemtableNum)
			if sstRecords != nil {
				err := utils.WriteToDisk(sstRecords, sstableDir, bm, &lsm, cfg, dict, dictPath)
				if err != nil {
					fmt.Println("Greska pri pisanju SSTable:", err)
				}
			}
			_, inCache := lru.CheckCache(key)
			if inCache {
				lru.DeleteFromCache(key)
			}
			fmt.Println("HLL obrisan:", name)

		// -----------------------------------
		// SIMHASH
		// -----------------------------------

		case "SIMHASH_CREATE":
			if len(parts) != 3 {
				fmt.Println("Greska: SIMHASH_CREATE zahteva <ime> <putanja_do_fajla>")
				continue
			}

			name := parts[1]
			filepath := parts[2]
			text, err := probabilistic.ReadFile(filepath)
			if err != nil {
				fmt.Println("Greska pri citanju fajla:", err)
				continue
			}

			wordWeights := probabilistic.GetWordWeights(text)
			fingerprint := probabilistic.ComputeSimhash(wordWeights)

			// Serijalizacija i zapis
			value := make([]byte, 8)
			binary.BigEndian.PutUint64(value, fingerprint)
			key := "__sys__prob__sim__" + name

			ts, err := walInstance.AppendRecord(false, []byte(key), value)
			if err != nil {
				fmt.Println("Greska pri pisanju u WAL:", err)
				continue
			}

			sstRecords := utils.WriteToMemory(ts, false, key, value, bm, &memtableInstances, &mtIndex, walInstance, cfg.MemtableNum)
			if sstRecords != nil {
				err := utils.WriteToDisk(sstRecords, sstableDir, bm, &lsm, cfg, dict, dictPath)
				if err != nil {
					fmt.Println("Greska pri pisanju SSTable:", err)
				}
			}

			fmt.Println("SimHash fingerprint sačuvan pod imenom:", name)

		case "SIMHASH_DISTANCE":
			if len(parts) != 3 {
				fmt.Println("Greska: SIMHASH_DISTANCE zahteva <ime1> <ime2>")
				continue
			}

			name1 := parts[1]
			name2 := parts[2]

			key1 := "__sys__prob__sim__" + name1
			key2 := "__sys__prob__sim__" + name2

			var data1, data2 []byte
			var found1, found2 bool
			var deleted1, deleted2 bool

			// Prvo memtable pretraga
			for i := 0; i < cfg.MemtableNum; i++ {
				data1, deleted1, found1 = memtableInstances[i].Get(key1)
				if found1 && !deleted1 {
					break
				}
			}
			for i := 0; i < cfg.MemtableNum; i++ {
				data2, deleted2, found2 = memtableInstances[i].Get(key2)
				if found2 && !deleted2 {
					break
				}
			}

			// Ako nije nadjeno, idi u Cache
			if !found1 {
				data1, found1 = lru.CheckCache(key1)
			}
			if !found2 {
				data2, found2 = lru.CheckCache(key2)
			}

			// Ako nije nadjeno, idi u SSTable
			if (!found1 || deleted1) || (!found2 || deleted2) {
				maxLevel := byte(0)
				for level := range lsm {
					if level > maxLevel {
						maxLevel = level
					}
				}

				if !found1 || deleted1 {
					record1 := utils.ReadFromDisk(key1, maxLevel, lsm, cfg, bm, dict)
					if record1 != nil {
						data1 = record1.Value
						lru.UpdateCache(key1, data1)
					}

				}

				if !found2 || deleted2 {
					record2 := utils.ReadFromDisk(key1, maxLevel, lsm, cfg, bm, dict)
					if record2 != nil {
						data2 = record2.Value
						lru.UpdateCache(key2, data2)
					}
				}
			}

			if data1 == nil || data2 == nil {
				fmt.Println("Greska: Jedan od SimHash fingerprint-a nije pronadjen.")
				continue
			}

			hash1 := binary.BigEndian.Uint64(data1)
			hash2 := binary.BigEndian.Uint64(data2)

			dist := probabilistic.HammingDistance(hash1, hash2)
			fmt.Printf("Hamming distanca izmedju '%s' i '%s' je: %d\n", name1, name2, dist)

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
					newCursor, err := sstable.NewCursor(bm, path, minKey, maxKey, cfg.BlockSize, cfg.SSTableCompression, dict)
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
					newCursor, err := sstable.NewCursor(bm, path, minKey, maxKey, cfg.BlockSize, cfg.SSTableCompression, dict)
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
					newCursor, err := sstable.NewCursor(bm, path, minKey, maxKey, cfg.BlockSize, cfg.SSTableCompression, dict)
					if err != nil {
						fmt.Printf("Greška prilikom formiranja kursora.")
					}
					sstableCursors = append(sstableCursors, &newCursor)
				}
			}

			cursors = append(cursors, sstableCursors...)

			// Napravi jedan multi cursor kao wrapper svih cursora
			mc := cursor.NewMultiCursor(minKey, maxKey, cursors...)

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
			mc.Close() // Zatvori multicursor jer smo vec procitali sve podatke

			// Sortiraj kljuceve
			sortedKeys := make([]string, 0, len(records))
			for k := range records {
				if k >= minKey && k <= maxKey {
					sortedKeys = append(sortedKeys, k)
				}
			}
			sort.Strings(sortedKeys)

			// Iterate petlja koja koristi vec ucitane zapise
			currentIndex := 0
		outer_prefix:
			for currentIndex < len(sortedKeys) {
				key := sortedKeys[currentIndex]
				record := records[key]

				fmt.Printf("- [%s -> %s]\n", utils.MaybeQuote(key), utils.MaybeQuote(string(record.value)))
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
					currentIndex++
				default:
					fmt.Println("Nepoznata komanda. Upotrebite NEXT ili STOP")
				}
			}
			fmt.Printf("Izlaz iz iterate petlje.\n")

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
					newCursor, err := sstable.NewCursor(bm, path, minKey, maxKey, cfg.BlockSize, cfg.SSTableCompression, dict)
					if err != nil {
						fmt.Printf("Greška prilikom formiranja kursora.")
					}
					sstableCursors = append(sstableCursors, &newCursor)
				}
			}

			cursors = append(cursors, sstableCursors...)

			// Napravi jedan multi cursor kao wrapper svih cursora
			mc := cursor.NewMultiCursor(minKey, maxKey, cursors...)

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
			mc.Close() // Zatvori multicursor jer smo vec procitali sve podatke

			// Sortiraj kljuceve
			sortedKeys := make([]string, 0, len(records))
			for k := range records {
				if k >= minKey && k <= maxKey {
					sortedKeys = append(sortedKeys, k)
				}
			}
			sort.Strings(sortedKeys)

			// Iterate petlja koja koristi vec ucitane zapise
			currentIndex := 0
		outer_range:
			for currentIndex < len(sortedKeys) {
				key := sortedKeys[currentIndex]
				record := records[key]

				fmt.Printf("- [%s -> %s]\n", utils.MaybeQuote(key), utils.MaybeQuote(string(record.value)))
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
					currentIndex++
				default:
					fmt.Println("Nepoznata komanda. Upotrebite NEXT ili STOP")
				}
			}
			fmt.Printf("Izlaz iz iterate petlje.\n")

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
