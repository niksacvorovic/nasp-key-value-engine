package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"projekat/config"
	//"projekat/structs/blockmanager"
	"projekat/structs/containers"
	"projekat/structs/lrucache"
	"projekat/structs/memtable"
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

	// Unmarshal JSON podatek u Config strukturu
	err = json.Unmarshal(data, &cfg)
	if err != nil {
		log.Fatal(err)
	}

	// Inicijalizacija LRU keša
	lru := lrucache.NewLRUCache(cfg.LRUCacheSize)

	memtableInstances := make([]memtable.MemtableInterface, cfg.MemtableNum)
	mtIndex := 0

	// Inicijalizacija niza instanci Memtable-a
	if cfg.Memtable_struct == "hashMap" {
		for i := 0; i < cfg.MemtableNum; i++ {
			memtableInstances[i] = containers.NewHashMapMemtable(cfg.MaxMemtableSize)
		}
	} else if cfg.Memtable_struct == "skipList" {
		for i := 0; i < cfg.MemtableNum; i++ {
			memtableInstances[i] = containers.NewSkipListMemtable(cfg.SkipListLevelNum, cfg.MaxMemtableSize)
		}

	} else if cfg.Memtable_struct == "BStablo" {
		// NewBStabloMemtable
	}

	// -------------------------------------------------------------------------------------------------------------------------------
	// WAL (Write Ahead Log)
	// -------------------------------------------------------------------------------------------------------------------------------

	// Put do foldera sa wal logovima
	walDir := filepath.Join("data", "wal")

	// Inicijalizacija WAL-a
	walInstance, err := wal.NewWAL(walDir, cfg.WalMaxSeg)
	if err != nil {
		log.Fatalf("Greška pri inicijalizaciji WAL-a: %v", err)
	}
	// defer walInstance.Close()

	// Ucitavanje podataka is WAL-a u Memtable
	files, err := os.ReadDir(walInstance.Dir)
	if err != nil {
		panic(err)
	}

	for _, f := range files {
		if strings.HasPrefix(f.Name(), "wal_") && strings.HasSuffix(f.Name(), ".log") {
			segmentPath := filepath.Join(walInstance.Dir, f.Name())

			recordChan, errChan := walInstance.ReadRecords(segmentPath)

			for record := range recordChan {
				// Dodavanje u Memtable
				memtableInstances[mtIndex].Add(string(record.Key), record.Value)

				// Provera da li je trenutni Memtable pun
				if memtableInstances[mtIndex].IsFull() && mtIndex != cfg.MemtableNum-1 {
					// Ako je trenutni Memtable pun i nije poslednji, prelazi se na sledeci Memtable
					mtIndex++
					continue
				} else if memtableInstances[mtIndex].IsFull() && mtIndex == cfg.MemtableNum-1 {
					// Ako su svi Memtable-ovi puni, pokrece se serijalizacija u SSTable
					tables := make([][]memtable.Record, cfg.MemtableNum)
					for i := 0; i < cfg.MemtableNum; i++ {
						// Flushovanje i serijalizacija svakog Memtable-a u SSTable fajl
						tables = append(tables, *memtableInstances[i].Flush())
						// ovde dodati izgradnju sstabele
					}
				}

			}

			if err := <-errChan; err != nil && err != io.EOF {
				fmt.Println("Greška pri čitanju:", err)
			}
		}
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
				memtableInstances[0].Add("_TOKEN_BUCKET", newbucket)
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
			memtableInstances[tokenIndex].Add("_TOKEN_BUCKET", bucket)
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
			err = walInstance.AppendRecord(tombstone, key, value)
			if err != nil {
				// Ako dodje do greske prilikom upisa u WAL, ispisuje se poruka o gresci
				fmt.Printf("Greška prilikom pisanja u WAL: %v\n", err)
			} else {
				// Ako je upis u WAL uspesan, dodaje se u Memtable
				memtableInstances[mtIndex].Add(parts[1], value)
				fmt.Printf("Uspešno dodato u WAL i Memtable: [%s -> %s]\n", key, value)

				// Provera da li je trenutni Memtable pun
				if memtableInstances[mtIndex].IsFull() && mtIndex != cfg.MemtableNum-1 {
					// Ako je trenutni Memtable pun i nije poslednji, prelazi se na sledeci Memtable
					fmt.Println("Dostignuta maksimalna veličina Memtable-a, prelazak na sledeći...")
					mtIndex++
					continue
				} else if memtableInstances[mtIndex].IsFull() && mtIndex == cfg.MemtableNum-1 {
					// Ako su svi Memtable-ovi puni, pokrece se serijalizacija u SSTable
					fmt.Println("Popunjeni svi Memtable-ovi, serijalizacija u SSTable...")
					tables := make([][]memtable.Record, cfg.MemtableNum)
					for i := 0; i < cfg.MemtableNum; i++ {
						// Flushovanje i serijalizacija svakog Memtable-a u SSTable fajl
						tables = append(tables, *memtableInstances[i].Flush())
						// ovde dodati izgradnju sstabele
					}
				}
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
			for i := 0; i < cfg.MemtableNum; i++ {
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
				break
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
				err = walInstance.AppendRecord(tombstone, key, value)
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
