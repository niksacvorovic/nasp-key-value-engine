package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
)

type Dictionary struct {
	strToID map[string]uint64
	idToStr map[uint64]string
	nextID  uint64
}

// NewDictionary pravi novi prazan rečnik
func NewDictionary() *Dictionary {
	return &Dictionary{
		strToID: make(map[string]uint64),
		idToStr: make(map[uint64]string),
		nextID:  1,
	}
}

// GetID vraća ID za ključ, kreira novi ako ne postoji
func (d *Dictionary) GetID(key string) uint64 {
	if id, ok := d.strToID[key]; ok {
		return id
	}
	id := d.nextID
	d.nextID++
	d.strToID[key] = id
	d.idToStr[id] = key
	return id
}

// Lookup vraća originalni ključ za dati ID
func (d *Dictionary) Lookup(id uint64) (string, error) {
	val, ok := d.idToStr[id]
	if !ok {
		return "", errors.New("key id not found")
	}
	return val, nil
}

// SaveToFile serijalizuje rečnik u datoteku
func (d *Dictionary) SaveToFile(path string) error {
	buf := &bytes.Buffer{}

	for key, id := range d.strToID {
		tmp := make([]byte, binary.MaxVarintLen64)

		n := binary.PutUvarint(tmp, id)
		buf.Write(tmp[:n])

		n = binary.PutUvarint(tmp, uint64(len(key)))
		buf.Write(tmp[:n])

		buf.Write([]byte(key))
	}

	return os.WriteFile(path, buf.Bytes(), 0644)
}

// LoadFromFile deserijalizuje rečnik iz datoteke
func (d *Dictionary) LoadFromFile(path string) error {
	d.strToID = make(map[string]uint64)
	d.idToStr = make(map[uint64]string)
	d.nextID = 1

	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	r := bytes.NewReader(data)
	for {
		id, err := binary.ReadUvarint(r)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		keyLen, err := binary.ReadUvarint(r)
		if err != nil {
			return err
		}

		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(r, keyBytes); err != nil {
			return err
		}

		key := string(keyBytes)
		d.strToID[key] = id
		d.idToStr[id] = key
		if id >= d.nextID {
			d.nextID = id + 1
		}
	}
	return nil
}
