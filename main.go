package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"projekat/structs/wal"
	"projekat/structs/memtable"
)

func main() {

	// Definisanje putanje do log fajla i velicine segmenta
	walFilePath := "wal.log"
	maxSegmentSize := 10
	maxMemtableSize := 10

	// Inicijalizuj WAL
	wal, err := wal.NewWAL(walFilePath, maxSegmentSize)
	if err != nil {
		fmt.Printf("Greska prilikom inicijalizovanja WAL: %v\n", err)
		return
	}
	defer wal.Close()

	// Inicijalizacija memtable i popunjavanje sa podacima iz WAL
	memtable := memtable.NewMemtable(maxMemtableSize)
	err = memtable.LoadFromWAL(walFilePath)
	if err != nil {
		fmt.Println("Greska pri ucitavanju iz WAL-a", err)
		return
	}

	// TEMP
	// Prikaz sadrzaja Memtable-a
	memtable.PrintData()
	fmt.Println()

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
				err = memtable.Add(parts[1], parts[2])
				if err != nil {
					fmt.Printf("Greska prilikom pisanja u Memtable: %v\n", err)
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
			value, found := memtable.Get(parts[1])
			if found {
				fmt.Printf("Vrednost za kljuc: [%s -> %s]\n", parts[1], value)
			} else {
				fmt.Printf("Nema podataka za kljuc: %s\n", parts[1])
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
			value, found := memtable.Get(parts[1])
			tombstone := true

			// Izbrisi iz WAL-a i Memtable-a
			if found {
				err = wal.AppendRecord(tombstone, key, []byte(value))
				if err != nil {
					fmt.Printf("Greska prilikom brisanja iz WAL-a: [%s -> %s]\n", key, value)
				} else {
					err := memtable.Delete(parts[1])
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
			// TEMP
			// Prikaz sadrzaja Memtable-a
			fmt.Println()
			memtable.PrintData()
			return

		default:
			fmt.Println("Nepoznata komanda. Unesite 'help' za listu dostupnih komandi.")
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Greska pri citanju unosa:", err)
	}
}
