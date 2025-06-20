package blockmanager

import (
	"errors"
	"io"
	"os"
)

// BlockManager struktura
type BlockManager struct {
	blockCache *BlockCache
	blockSize  int
	Block_idx  int
}

// Funckija koja vraća novi Block Manager
func NewBlockManager(blockSize int, capacity int) *BlockManager {
	return &BlockManager{
		blockCache: NewBlockCache(capacity), blockSize: blockSize,
		Block_idx: 0,
	}
}

// Funkcija za citanje blokova
func (bm *BlockManager) ReadBlock(filePath string, blockIndex int) ([]byte, error) {
	sign := Signature{filePath, blockIndex}

	// Ako postoji u kesu
	if blockptr, ok := bm.blockCache.hash[sign]; ok {
		return blockptr.data, nil
	}

	// Otvori fajl
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Pripremi buffer
	data := make([]byte, bm.blockSize)

	// Pročitaj sa tacnog offseta
	offset := int64(blockIndex * bm.blockSize)
	_, err = file.ReadAt(data, offset)
	if err != nil && err != io.EOF {
		return nil, err
	}

	// Dodaj u kes
	bm.blockCache.AddToCache(filePath, blockIndex, data)

	return data, nil
}

// Funkcija za pisanje blokova
func (bm *BlockManager) WriteBlock(filePath string, data []byte) error {
	// Greška: podaci su veći od veličine bloka
	if len(data) > bm.blockSize {
		return errors.New("data does not fit into a block")
	}

	// Dodaj padding da blokovi budu uniformne dužine
	padded := make([]byte, bm.blockSize)
	copy(padded, data)

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Zapisi blok u fajl
	offset := int64(bm.Block_idx * bm.blockSize)
	_, err = file.WriteAt(padded, offset)
	if err != nil {
		return err
	}

	// Azuriraj block_idx
	bm.Block_idx++

	// Ažuriraj cache ako postoji
	sign := Signature{filePath, bm.Block_idx - 1}
	if block, ok := bm.blockCache.hash[sign]; ok {
		block.data = padded
	}

	return nil
}
