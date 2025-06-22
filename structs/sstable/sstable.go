package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"
	"time"

	"projekat/structs/merkletree"
	"projekat/structs/probabilistic"
)

// Struktura jednog data bloka
type Record struct {
	CRC       uint32
	Timestamp int64
	Tombstone bool
	KeySize   uint64
	ValueSize uint64
	Key       []byte
	Value     []byte
}

// Struktura index zapisa vezanog za data blok
type Index struct {
	Key    []byte
	Offset uint64
}

// Struktura summary podzapisa vezanog za index blok
type SummaryEntry struct {
	Key    []byte
	Offset uint64
}

// Struktura summary zapisa
type Summary struct {
	MinKey  []byte
	MaxKey  []byte
	Entries []SummaryEntry
}

// Struktura SSTable
type SSTable struct {
	DataFilePath     string
	IndexFilePath    string
	SummaryFilePath  string
	FilterFilePath   string
	MetadataFilePath string

	Filter   *probabilistic.BloomFilter
	Metadata *merkletree.MerkleTree
}

// Racunanje CRC32 za validaciju zapisa data bloka
func calculateCRC(record Record) uint32 {
	buffer := bytes.Buffer{}
	binary.Write(&buffer, binary.LittleEndian, record.Timestamp)
	binary.Write(&buffer, binary.LittleEndian, record.Tombstone)
	binary.Write(&buffer, binary.LittleEndian, record.KeySize)
	binary.Write(&buffer, binary.LittleEndian, record.ValueSize)
	buffer.Write(record.Key)
	buffer.Write(record.Value)
	return crc32.ChecksumIEEE(buffer.Bytes())
}

// Kreiranje SSTable sa Bloom filterom, Merkle stablom i summary zapisima
func CreateSSTable(records []Record, sst *SSTable) error {
	// Sortiranje po ključu
	sort.Slice(records, func(i, j int) bool {
		return bytes.Compare(records[i].Key, records[j].Key) < 0
	})

	// Otvaranje fajlova
	dataFile, err := os.Create(sst.DataFilePath)
	if err != nil {
		return err
	}
	defer dataFile.Close()

	indexFile, err := os.Create(sst.IndexFilePath)
	if err != nil {
		return err
	}
	defer indexFile.Close()

	summaryFile, err := os.Create(sst.SummaryFilePath)
	if err != nil {
		return err
	}
	defer summaryFile.Close()

	filterFile, err := os.Create(sst.FilterFilePath)
	if err != nil {
		return err
	}
	defer filterFile.Close()

	// Kreiranje Bloom filtera
	bloom := probabilistic.CreateBF(len(records), 0.01)

	summaryEntries := []SummaryEntry{}
	var currentOffset uint64 = 0
	blockSize := 4

	for i, record := range records {
		record.Timestamp = time.Now().UnixNano()
		record.CRC = calculateCRC(record)

		binary.Write(dataFile, binary.LittleEndian, record.CRC)
		binary.Write(dataFile, binary.LittleEndian, record.Timestamp)
		binary.Write(dataFile, binary.LittleEndian, record.Tombstone)
		binary.Write(dataFile, binary.LittleEndian, record.KeySize)
		binary.Write(dataFile, binary.LittleEndian, record.ValueSize)
		dataFile.Write(record.Key)
		dataFile.Write(record.Value)

		bloom.AddElement(string(record.Key))

		binary.Write(indexFile, binary.LittleEndian, record.KeySize)
		indexFile.Write(record.Key)
		binary.Write(indexFile, binary.LittleEndian, currentOffset)

		if i%blockSize == 0 {
			summaryEntries = append(summaryEntries, SummaryEntry{
				Key:    record.Key,
				Offset: currentOffset,
			})
		}

		currentOffset += uint64(4 + 8 + 1 + 8 + 8 + len(record.Key) + len(record.Value))
	}

	// Upis summary fajla
	minKey := records[0].Key
	maxKey := records[len(records)-1].Key

	binary.Write(summaryFile, binary.LittleEndian, uint64(len(minKey)))
	summaryFile.Write(minKey)

	binary.Write(summaryFile, binary.LittleEndian, uint64(len(maxKey)))
	summaryFile.Write(maxKey)

	binary.Write(summaryFile, binary.LittleEndian, uint64(len(summaryEntries)))

	for _, entry := range summaryEntries {
		binary.Write(summaryFile, binary.LittleEndian, uint64(len(entry.Key)))
		summaryFile.Write(entry.Key)
		binary.Write(summaryFile, binary.LittleEndian, entry.Offset)
	}

	// Serijalizacija Bloom filtera
	filterBytes := bloom.Serialize()
	_, err = filterFile.Write(filterBytes)
	if err != nil {
		return err
	}

	// Kreiranje Merkle stabla pomoću tvoje logike
	dataFileForMerkle, err := os.Open(sst.DataFilePath)
	if err != nil {
		return err
	}
	defer dataFileForMerkle.Close()

	data, err := io.ReadAll(dataFileForMerkle)
	if err != nil {
		return err
	}

	merkleTree := merkletree.NewMerkleTree()
	merkleTree.ConstructMerkleTree(data, 64)

	// Serijalizacija u bajt niz i upis u fajl
	metadataFile, err := os.Create(sst.MetadataFilePath)
	if err != nil {
		return err
	}
	defer metadataFile.Close()

	merkleBytes := merkleTree.Serialize()
	_, err = metadataFile.Write(merkleBytes)
	if err != nil {
		return err
	}

	// Postavi Bloom filter i Merkle stablo u SSTable
	sst.Filter = &bloom
	sst.Metadata = &merkleTree

	return nil
}

// Zapisivanje data bloka u datoteku sa sortiranjem, timestampom i CRC-om
func WriteDataFile(records []Record, filePath string) error {
	// Sortiranje pre pisanja
	sort.Slice(records, func(i, j int) bool {
		return bytes.Compare(records[i].Key, records[j].Key) < 0
	})

	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	for _, record := range records {
		record.Timestamp = time.Now().UnixNano()
		record.CRC = calculateCRC(record)

		if err := binary.Write(file, binary.LittleEndian, record.CRC); err != nil {
			return err
		}
		if err := binary.Write(file, binary.LittleEndian, record.Timestamp); err != nil {
			return err
		}
		if err := binary.Write(file, binary.LittleEndian, record.Tombstone); err != nil {
			return err
		}
		if err := binary.Write(file, binary.LittleEndian, record.KeySize); err != nil {
			return err
		}
		if err := binary.Write(file, binary.LittleEndian, record.ValueSize); err != nil {
			return err
		}
		if _, err := file.Write(record.Key); err != nil {
			return err
		}
		if _, err := file.Write(record.Value); err != nil {
			return err
		}
	}

	return nil
}

// Citanje data zapisa uz CRC proveru
func ReadRecordAtOffset(filePath string, offset int64) (*Record, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, err
	}

	record := Record{}

	if err := binary.Read(file, binary.LittleEndian, &record.CRC); err != nil {
		return nil, err
	}
	if err := binary.Read(file, binary.LittleEndian, &record.Timestamp); err != nil {
		return nil, err
	}
	if err := binary.Read(file, binary.LittleEndian, &record.Tombstone); err != nil {
		return nil, err
	}
	if err := binary.Read(file, binary.LittleEndian, &record.KeySize); err != nil {
		return nil, err
	}
	if err := binary.Read(file, binary.LittleEndian, &record.ValueSize); err != nil {
		return nil, err
	}

	record.Key = make([]byte, record.KeySize)
	if _, err := file.Read(record.Key); err != nil {
		return nil, err
	}

	record.Value = make([]byte, record.ValueSize)
	if _, err := file.Read(record.Value); err != nil {
		return nil, err
	}

	calculatedCRC := calculateCRC(record)
	if calculatedCRC != record.CRC {
		return nil, errors.New("CRC check failed: data is corrupted")
	}

	return &record, nil
}

// Pisanje index datoteke
func WriteIndexFile(indexes []Index, filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	for _, idx := range indexes {
		keySize := uint64(len(idx.Key))
		if err := binary.Write(file, binary.LittleEndian, keySize); err != nil {
			return err
		}
		if _, err := file.Write(idx.Key); err != nil {
			return err
		}
		if err := binary.Write(file, binary.LittleEndian, idx.Offset); err != nil {
			return err
		}
	}

	return nil
}

// Citanje index bloka iz index fajla
func ReadIndexBlock(filePath string, blockSize int64, offset int64) ([]Index, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, err
	}

	buffer := make([]byte, blockSize)
	bytesRead, err := file.Read(buffer)
	if err != nil && err != io.EOF {
		return nil, err
	}

	indexes := []Index{}
	reader := bytes.NewReader(buffer[:bytesRead])

	for reader.Len() > 0 {
		var keySize uint64
		if err := binary.Read(reader, binary.LittleEndian, &keySize); err != nil {
			break
		}

		key := make([]byte, keySize)
		if _, err := reader.Read(key); err != nil {
			break
		}

		var offset uint64
		if err := binary.Read(reader, binary.LittleEndian, &offset); err != nil {
			break
		}

		indexes = append(indexes, Index{Key: key, Offset: offset})
	}

	return indexes, nil
}

// Validacija Merkle stabla
func ValidateMerkleTree(sst *SSTable, blockSize int) (bool, error) {
	file, err := os.Open(sst.DataFilePath)
	if err != nil {
		return false, err
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return false, err
	}

	newTree := merkletree.NewMerkleTree()
	newTree.ConstructMerkleTree(data, blockSize)

	if bytes.Equal(newTree.MerkleRoot.Hash, sst.Metadata.MerkleRoot.Hash) {
		return true, nil
	}
	return false, errors.New("merkle tree validation failed")
}

// Pretraga kljuca u SSTable
func Search(sst *SSTable, key []byte, summary Summary) (*Record, error) {
	if !sst.Filter.IsAdded(string(key)) {
		return nil, fmt.Errorf("key not found (Bloom filter)")
	}

	if bytes.Compare(key, summary.MinKey) < 0 || bytes.Compare(key, summary.MaxKey) > 0 {
		return nil, fmt.Errorf("key not in summary range")
	}

	indexOffset := FindIndexBlockOffset(summary, key)

	indexes, err := ReadIndexBlock(sst.IndexFilePath, 4096, indexOffset)
	if err != nil {
		return nil, err
	}

	var dataOffset uint64
	found := false
	for _, idx := range indexes {
		if bytes.Equal(idx.Key, key) {
			dataOffset = idx.Offset
			found = true
			break
		}
	}

	if !found {
		return nil, fmt.Errorf("key not found in index block")
	}

	record, err := ReadRecordAtOffset(sst.DataFilePath, int64(dataOffset))
	if err != nil {
		return nil, err
	}

	return record, nil
}

// Pronalazenje offset-a u summary fajlu
func FindIndexBlockOffset(summary Summary, key []byte) int64 {
	for i := len(summary.Entries) - 1; i >= 0; i-- {
		if bytes.Compare(summary.Entries[i].Key, key) <= 0 {
			return int64(summary.Entries[i].Offset)
		}
	}
	return 0
}

// Ucitavanje Bloom filtera iz fajla
func LoadBloomFilter(filePath string) (*probabilistic.BloomFilter, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	bf := &probabilistic.BloomFilter{}
	err = bf.Deserialize(file)
	if err != nil {
		return nil, err
	}

	return bf, nil
}

// Ucitavanje summary fajla
func LoadSummary(filePath string) (Summary, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return Summary{}, err
	}
	defer file.Close()

	var minKeySize uint64
	binary.Read(file, binary.LittleEndian, &minKeySize)
	minKey := make([]byte, minKeySize)
	file.Read(minKey)

	var maxKeySize uint64
	binary.Read(file, binary.LittleEndian, &maxKeySize)
	maxKey := make([]byte, maxKeySize)
	file.Read(maxKey)

	var entryCount uint64
	binary.Read(file, binary.LittleEndian, &entryCount)

	entries := make([]SummaryEntry, 0)
	for i := 0; i < int(entryCount); i++ {
		var keySize uint64
		binary.Read(file, binary.LittleEndian, &keySize)
		key := make([]byte, keySize)
		file.Read(key)

		var offset uint64
		binary.Read(file, binary.LittleEndian, &offset)

		entries = append(entries, SummaryEntry{Key: key, Offset: offset})
	}

	return Summary{MinKey: minKey, MaxKey: maxKey, Entries: entries}, nil
}

// Ucitavanje Merkle stabla iz fajla
func LoadMerkleTree(filePath string) (*merkletree.MerkleTree, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	tree := merkletree.NewMerkleTree()
	err = tree.Deserialize(file)
	if err != nil {
		return nil, err
	}

	return &tree, nil
}

// Iniciranje validacije Merkle stabla
func InitiateValidation(dataPath string, metadataPath string, blockSize int) (bool, error) {
	file, err := os.Open(dataPath)
	if err != nil {
		return false, err
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return false, err
	}

	newTree := merkletree.NewMerkleTree()
	newTree.ConstructMerkleTree(data, blockSize)

	metadataFile, err := os.Open(metadataPath)
	if err != nil {
		return false, err
	}
	defer metadataFile.Close()

	oldTree := merkletree.NewMerkleTree()
	err = oldTree.Deserialize(metadataFile)
	if err != nil {
		return false, err
	}

	if bytes.Equal(newTree.MerkleRoot.Hash, oldTree.MerkleRoot.Hash) {
		return true, nil
	}
	return false, errors.New("merkle validation failed: data has been modified")
}
