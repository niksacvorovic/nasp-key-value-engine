package blockmanager

import (
	"errors"
	"os"
)

// BlockManager struktura
type BlockManager struct {
	blockCache *BlockCache
	blockSize  int
}

// Funckija koja vraća novi Block Manager
func NewBlockManager(blockSize int, capacity int) *BlockManager {
	return &BlockManager{blockCache: NewBlockCache(capacity), blockSize: blockSize}
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
func (bm *BlockManager) WriteBlock(filePath string, blockIndex int, data []byte) error {

	if len(data) != bm.blockSize {
		return errors.New("data size does not match block size")
	}

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	_, err = file.WriteAt(data, int64(blockIndex*bm.blockSize))
	if err != nil {
		panic(err)
	}

	sign := Signature{filePath, blockIndex}
	if _, ok := bm.blockCache.hash[sign]; ok {
		bm.blockCache.hash[sign].data = data
	}

	return nil
}
