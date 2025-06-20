package blockmanager

import (
	"errors"
	"os"
)

// BlockManager struktura
type BlockManager struct {
	blockCache *BlockCache
	blockSize  int
	block_idx  int
}

// Funckija koja vraća novi Block Manager
func NewBlockManager(blockSize int, capacity int) *BlockManager {
	return &BlockManager{
		blockCache: NewBlockCache(capacity), blockSize: blockSize,
		block_idx: 0,
	}
}

// Funckija za citanje blokova
func (bm *BlockManager) ReadBlock(filePath string, blockIndex int) []byte {

	sign := Signature{filePath, blockIndex}

	blockptr, ok := bm.blockCache.hash[sign]
	if ok {
		return blockptr.data
	}

	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	data := make([]byte, bm.blockSize)
	_, err = file.ReadAt(data, int64(blockIndex*bm.blockSize))
	if err != nil {
		panic(err)
	}

	bm.blockCache.AddToCache(filePath, blockIndex, data)

	return data
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
	offset := int64(bm.block_idx * bm.blockSize)
	_, err = file.WriteAt(padded, offset)
	if err != nil {
		return err
	}

	// Ažuriraj block_idx
	bm.block_idx++

	// Ažuriraj cache ako postoji
	sign := Signature{filePath, bm.block_idx - 1}
	if block, ok := bm.blockCache.hash[sign]; ok {
		block.data = padded
	}

	return nil
}
