package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"projekat/config"
	"projekat/structs/blockmanager"
	"projekat/structs/containers"
	"projekat/structs/lrucache"
	"projekat/structs/memtable"
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
	memtableInstances := make([]memtable.MemtableInterface, cfg.Num_memtables)
	mtIndex := 0

	// Inicijalizacija niza instanci Memtable-a
	switch cfg.Memtable_struct {
	case "hashMap":
		for i := 0; i < cfg.Num_memtables; i++ {
			memtableInstances[i] = containers.NewHashMapMemtable(cfg.MaxMemtableSize)
		}
	case "skipList":
		for i := 0; i < cfg.Num_memtables; i++ {
			memtableInstances[i] = containers.NewSkipListMemtable(cfg.SkipListLevelNum, cfg.MaxMemtableSize)
		}

	case "BStablo":
		for i := 0; i < cfg.Num_memtables; i++ {
			memtableInstances[i] = containers.NewBTreeMemtable(cfg.MaxMemtableSize)
		}
	}

	// -------------------------------------------------------------------------------------------------------------------------------
	// WAL (Write Ahead Log)
	// -------------------------------------------------------------------------------------------------------------------------------

	// Put do foldera sa wal logovima
	walDir := filepath.Join("data", "wal")

	// Inicijalizacija WAL-a
	walInstance, err := wal.NewWAL(walDir, cfg.WalMaxRecordsPerSegment, cfg.WalBlokcsPerSegment, cfg.BlockSize, cfg.BlockCacheSize)
	if err != nil {
		log.Fatalf("Greška pri inicijalizaciji WAL-a: %v", err)
	}

	// Citanje WAL fajlova
	recordMap, err := walInstance.ReadRecords()
	if err != nil {
		fmt.Println("Greška pri čitanju:", err)
	}

	// Dodavanje WAL zapisa u Memtabele
	for walIndex := walInstance.FirstSeg; walIndex <= walInstance.SegNum; walIndex++ {
		records := recordMap[walIndex]
		for idx := range records {
			// Dodavanje zapisa u memtable
			memtableInstances[mtIndex].Add(records[idx].Timestamp, records[idx].Tombstone, string(records[idx].Key), records[idx].Value)

			// Postavljanje watermarka za svaki Memtable
			memtableInstances[mtIndex].SetWatermarks(walIndex)

			// Provera da li je trenutni Memtable pun
			if memtableInstances[mtIndex].IsFull() {
				mtIndex = (mtIndex + 1) % cfg.Num_memtables
				continue
			}
		}
	}

	// -------------------------------------------------------------------------------------------------------------------------------
	// SSTable i LSM stablo
	// -------------------------------------------------------------------------------------------------------------------------------

	// Putanja do foldera sa SSTable fajlovima
	sstableDir := filepath.Join("data", "sstable")

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
		tokenIndex := mtIndex
		if command == "GET" || command == "PUT" || command == "DELETE" {
			bucket, ok := memtableInstances[tokenIndex].Get("_TOKEN_BUCKET")
			if !ok {
				newtimestamp := uint64(time.Now().Unix())
				newtokens := uint8(cfg.TokenRate)
				newbucket := make([]byte, 0)
				newbucket = binary.BigEndian.AppendUint64(newbucket, newtimestamp)
				newbucket = append(newbucket, newtokens)
				memtableInstances[0].Add([16]byte{}, false, "_TOKEN_BUCKET", newbucket)
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
			memtableInstances[tokenIndex].Add([16]byte{}, false, "_TOKEN_BUCKET", bucket)
		}

		// --------------------------------------------------------------------------------------------------------------------------
		// PUT komanda
		// --------------------------------------------------------------------------------------------------------------------------

		switch command {
		// PUT komanda ocekuje 2 argumenta: key, value
		case "PUT":
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
				memtableInstances[mtIndex].SetWatermarks(walInstance.SegNum)
				// Proveravamo da li je trenutni memtable popunjen
				if memtableInstances[mtIndex].IsFull() {
					fmt.Println("Dostignuta maksimalna veličina Memtable-a, prelazak na sledeći...")
					mtIndex = (mtIndex + 1) % cfg.Num_memtables
					// Ako je i sledeći memtable pun - svi su puni
					// Flushujemo memtable i stavljamo njegov sadržaj u SSTable
					if memtableInstances[mtIndex].IsFull() {
						// Low watermark provera za brisanje WALa
						// Brišemo sve WAL segmente između low i high watermarka
						lowWatermark, highWatermark := memtableInstances[mtIndex].GetWatermarks()
						for i := lowWatermark; i < highWatermark; i++ {
							deletePath := walInstance.GetSegmentFilename(i)
							err := os.Remove(deletePath)
							if err != nil {
								fmt.Printf("Greška pri brisanju WAL segmenata")
							}
						}
						fmt.Println("Prevođenje sadržaja Memtable u SSTable")
						sstrecords := memtable.ConvertMemToSST(&memtableInstances[mtIndex])

						_, newSSTdir, err := sstable.CreateSSTable(sstrecords, sstableDir, cfg.SummaryStep, bm, cfg.BlockSize, 0, cfg.SSTableSingleFile)
						if err != nil {
							fmt.Printf("Greška pri kreiranju SSTable: %v\n", err)
						}
						lsm[0] = append(lsm[0], newSSTdir)
						// Funkcija za proveru i izvršenje kompakcija
						switch cfg.CompactionAlgorithm {
						case "SizeTiered":
							sstable.SizeTieredCompaction(bm, &lsm, sstableDir, cfg.MaxCountInLevel,
								cfg.BlockSize, cfg.SummaryStep, cfg.SSTableSingleFile)
						case "Leveled":
							sstable.LeveledCompaction(bm, &lsm, sstableDir, cfg.MaxCountInLevel,
								cfg.BlockSize, cfg.SummaryStep, cfg.SSTableSingleFile)
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
			if len(parts) != 2 {
				fmt.Println("Greska: GET zahteva <key>")
				continue
			}
			// Pretrazi Memtable po zadatom kljucu
			for i := 0; i < cfg.Num_memtables; i++ {
				value, found := memtableInstances[i].Get(parts[1])
				if found {
					fmt.Printf("Vrednost za kljuc: [%s -> %s]\n", parts[1], value)
					// Zapis u keš
					lru.UpdateCache(parts[1], value)
					break
				}
			}

			// Pretraga keša po zadatom ključu
			value, found := lru.CheckCache(parts[1])
			if found {
				fmt.Printf("Vrednost za kljuc: [%s -> %s]\n", parts[1], value)
				continue
			}
		// --------------------------------------------------------------------------------------------------------------------------
		// DELETE komanda
		// --------------------------------------------------------------------------------------------------------------------------

		// DELETE komanda ocekuje 1 argument: key
		case "DELETE":
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
			for delIndex < cfg.Num_memtables {
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
