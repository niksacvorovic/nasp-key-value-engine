package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"projekat/structs/blockmanager"
)

type Dictionary struct {
	strToID        map[string]uint64
	idToStr        map[uint64]string
	nextID         uint64
	unsavedBuffer  int
	flushThreshold int
}

// NewDictionary pravi novi prazan rečnik sa default pragom 100
func NewDictionary() *Dictionary {
	return &Dictionary{
		strToID:        make(map[string]uint64),
		idToStr:        make(map[uint64]string),
		nextID:         1,
		unsavedBuffer:  0,
		flushThreshold: 100,
	}
}

// NewDictionaryWithThreshold omogućava zadavanje vlastitog praga
func NewDictionaryWithThreshold(threshold int) *Dictionary {
	return &Dictionary{
		strToID:        make(map[string]uint64),
		idToStr:        make(map[uint64]string),
		nextID:         1,
		unsavedBuffer:  0,
		flushThreshold: threshold,
	}
}

// GetID vraća ID za ključ, kreira novi ako ne postoji i automatski čuva kad treba
func (d *Dictionary) GetID(key string, bm *blockmanager.BlockManager, path string, blockSize int) uint64 {
	if id, ok := d.strToID[key]; ok {
		return id
	}
	id := d.nextID
	d.nextID++
	d.strToID[key] = id
	d.idToStr[id] = key
	d.unsavedBuffer++

	if d.unsavedBuffer >= d.flushThreshold {
		_ = d.SaveToFile(path, bm, blockSize)
		d.unsavedBuffer = 0
	}
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

// SaveToFile serijalizuje rečnik u datoteku koristeći BlockManager
func (d *Dictionary) SaveToFile(path string, bm *blockmanager.BlockManager, blockSize int) error {
	buf := &bytes.Buffer{}
	tmp := make([]byte, binary.MaxVarintLen64)

	for key, id := range d.strToID {
		n := binary.PutUvarint(tmp, id)
		buf.Write(tmp[:n])

		n = binary.PutUvarint(tmp, uint64(len(key)))
		buf.Write(tmp[:n])

		buf.Write([]byte(key))
	}
	return writeBlocks(bm, path, buf.Bytes(), blockSize)
}

// LoadFromFile učitava rečnik iz datoteke koristeći BlockManager
func (d *Dictionary) LoadFromFile(path string, bm *blockmanager.BlockManager, blockSize int) error {
	d.strToID = make(map[string]uint64)
	d.idToStr = make(map[uint64]string)
	d.nextID = 1
	d.unsavedBuffer = 0

	fi, err := os.Stat(path)
	if err != nil {
		return err
	}
	size := int(fi.Size())

	data, err := readSegment(bm, path, 0, size, blockSize)
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

// ForceSaveToFile eksplicitno zapisuje ceo rečnik (npr. pri izlasku iz programa)
func (d *Dictionary) ForceSaveToFile(path string, bm *blockmanager.BlockManager, blockSize int) error {
	d.unsavedBuffer = 0
	return d.SaveToFile(path, bm, blockSize)
}
