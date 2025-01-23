package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"projekat/config"
	"projekat/structs/blockmanager"
	"projekat/structs/containers"
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

	// Unmarshal the JSON data into the Config struct
	err = json.Unmarshal(data, &cfg)
	if err != nil {
		log.Fatal(err)
	}

	// Definisanje putanje do log fajla
	walFilePath := "wal.log"

	// Inicijalizuj WAL
	wal, err := wal.NewWAL(walFilePath, cfg.WALmaxSegmentSize)
	if err != nil {
		fmt.Printf("Greska prilikom inicijalizovanja WAL: %v\n", err)
		return
	}
	defer wal.Close()

	// Inicijalizacija BlockManager-a
	bm := blockmanager.NewBlockManager(cfg.BlockSize, cfg.BlockCacheSize)

	memtableInstances := make([]memtable.MemtableInterface, cfg.Num_memtables)
	mtIndex := 0

	// Inicijalizacija niza instanci Memtable-a
	if cfg.Memtable_struct == "hashMap" {
		for i := 0; i < cfg.Num_memtables; i++ {
			memtableInstances[i] = containers.NewHashMapMemtable(cfg.MaxMemtableSize, bm)
		}
		fmt.Println("hashMap")
	} else if cfg.Memtable_struct == "skipList" {
		for i := 0; i < cfg.Num_memtables; i++ {
			memtableInstances[i] = containers.NewSkipListMemtable(cfg.SkipListLevelNum, cfg.MaxMemtableSize, bm)
		}
		fmt.Println("skipList")

	} else if cfg.Memtable_struct == "BStablo" {
		// NewBStabloMemtable
		fmt.Println("BStablo")
	}

	// Ucitavanje podataka is WAL-a u Memtable

	var offset int64 = 0
	file, err := os.Open(walFilePath)
	if err != nil {
		fmt.Errorf("ne mogu otvoriti WAL fajl: %v", err)
	}
	for {
		offset, err = memtableInstances[mtIndex].LoadFromWAL(file, offset)
		if err == memtable.MemtableFull {
			mtIndex++
			continue
		} else if err != nil {
			fmt.Println("Greska pri ucitavanju iz WAL-a", err)
			return
		} else {
			break
		}
	}

	file.Close()

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

		// --------------------------------------------------------------------------------------------------------------------------
		// PUT komanda
		// --------------------------------------------------------------------------------------------------------------------------

		switch command {
		// PUT komanda ocekuje 2 argumenta: key, value
		case "PUT":
			if len(parts) != 3 {
				fmt.Println("Greska: PUT zahteva <key> <value>")
				continue
			}

			// Konverzija kljuca i vrednosti u []byte, tombstone je false
			key := []byte(parts[1])
			value := []byte(parts[2])
			tombstone := false

			// Dodaj u WAL i Memtable
			err = wal.AppendRecord(tombstone, key, value)
			if err != nil {
				fmt.Printf("Greska prilikom pisanja u WAL: %v\n", err)
			} else {
				err = memtableInstances[mtIndex].Add(parts[1], parts[2])
				if err != nil && mtIndex != cfg.Num_memtables-1 {
					fmt.Println("Memtable reached max size, moving to next instance...")
					mtIndex++
					err = memtableInstances[mtIndex].Add(parts[1], parts[2])
					fmt.Printf("Uspesno dodato u WAL i Memtable: [%s -> %s]\n", key, value)
				} else if err != nil && mtIndex == cfg.Num_memtables-1 {
					fmt.Println("Serializing to SSTable...")
					// ovde dodati logiku kojom se kreira SSTable
				} else {
					fmt.Printf("Uspesno dodato u WAL i Memtable: [%s -> %s]\n", key, value)
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
			for i := 0; i < cfg.Num_memtables; i++ {
				value, found := memtableInstances[i].Get(parts[1])
				if found {
					fmt.Printf("Vrednost za kljuc: [%s -> %s]\n", parts[1], value)
					break
				}
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

			// Konverzija kljuca u []byte, tombstone je true
			key := []byte(parts[1])

			var value string
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
				err = wal.AppendRecord(tombstone, key, []byte(value))
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
