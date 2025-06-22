package blockmanager

import (
	"errors"
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

// Funckija za citanje blokova
func (bm *BlockManager) ReadBlock(filePath string, blockIndex int) ([]byte, error) {

	sign := Signature{filePath, blockIndex}

	blockptr, ok := bm.blockCache.hash[sign]
	if ok {
		return blockptr.data, nil
	}

	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	data := make([]byte, bm.blockSize)

	// Pročitaj sa tacnog offseta
	offset := int64(blockIndex * bm.blockSize)
	_, err = file.ReadAt(data, offset)
	if err != nil {
		return nil, err
	}

	bm.blockCache.AddToCache(filePath, blockIndex, data)

	return data, nil
}

// Funkcija za pisanje blokova
func (bm *BlockManager) WriteBlock(filePath string, data []byte) error {

	if len(data) != bm.blockSize {
		return errors.New("data size does not match block size")
	}

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	_, err = file.WriteAt(data, int64(bm.Block_idx*bm.blockSize))
	if err != nil {
		panic(err)
	}

	// Azuriraj block_idx
	bm.Block_idx++

	sign := Signature{filePath, bm.Block_idx}
	if _, ok := bm.blockCache.hash[sign]; ok {
		bm.blockCache.hash[sign].data = data
	}

	return nil
}
