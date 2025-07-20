package utils

import (
	"fmt"
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

func MaybeQuote(s string) string {
	if strings.ContainsAny(s, " \t\"") {
		escaped := strings.ReplaceAll(s, `"`, `\"`)
		return `"` + escaped + `"`
	}
	return s
}
