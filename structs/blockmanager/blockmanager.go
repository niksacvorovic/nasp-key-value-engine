package blockmanager

import(
	"errors"
	"os"
)

// BlockManager struktura
type BlockManager struct {
	blockSize int
}

// Funckija koja vraća novi Block Manager
func NewBlockManager(blockSize int) *BlockManager {
	return &BlockManager {blockSize: blockSize,}
}

// Funckija za citanje blokova
func (bm * BlockManager) ReadBlock(filePath string, blockIndex int) ([]byte) {

	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	data := make([]byte, bm.blockSize)
	_, err = file.ReadAt(data, int64(blockIndex * bm.BlockSize))
	if err != nil {
		panic(err)
	}

	return data 
}

// Funkcija za pisanje blokova
func (bm *BlockManager) WriteBlock(filePath string, blockIndex int, data []byte) error {

	if len(data) != bm.blockSize {
		return errors.New("data size does not match block size")
	}

	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	_, err = file.WriteAt(data, int64(blockIndex * bm.BlockSize))
	if err != nil {
		panic(err)
	}

	return nil
}