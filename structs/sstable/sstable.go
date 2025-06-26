package sstable

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"time"

	"projekat/structs/blockmanager"
	"projekat/structs/merkletree"
	"projekat/structs/probabilistic"
)

// Struktura jednog data bloka
type Record struct {
	CRC       uint32
	Timestamp [16]byte
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
	// Da li je SSTable u jednom fajlu ili više
	// Ako je SingleSSTable == true, onda se koristi samo SSTableFilePath
	SingleSSTable bool

	// Putanja do single SSTable fajla
	SingleFilePath string

	// Putanje do fajlova
	DataFilePath     string
	IndexFilePath    string
	SummaryFilePath  string
	FilterFilePath   string
	MetadataFilePath string

	// Pomoćne strukture
	Filter   *probabilistic.BloomFilter
	Metadata *merkletree.MerkleTree
}

// NewSingleFileSSTable kreira SSTable strukturu koja koristi samo jedan fajl za sve podatke.
func NewSingleFileSSTable(path string) *SSTable {
	ts := time.Now().UnixNano()
	return &SSTable{
		SingleFilePath: filepath.Join(path, fmt.Sprintf("%d-SSTable.db", ts)),
		SingleSSTable:  true,
	}
}

func NewMultiFileSSTable(path string) *SSTable {
	ts := time.Now().UnixNano()
	return &SSTable{
		SingleSSTable:    false,
		DataFilePath:     filepath.Join(path, fmt.Sprintf("%d-Data.db", ts)),
		IndexFilePath:    filepath.Join(path, fmt.Sprintf("%d-Index.db", ts)),
		SummaryFilePath:  filepath.Join(path, fmt.Sprintf("%d-Summary.db", ts)),
		FilterFilePath:   filepath.Join(path, fmt.Sprintf("%d-Filter.db", ts)),
		MetadataFilePath: filepath.Join(path, fmt.Sprintf("%d-Metadata.db", ts)),
	}
}

// writeBlocks deli ulazni bajt-niz na blokove veličine BlockManager-a i zapisuje svaki blok redom u datoteku.
func writeBlocks(bm *blockmanager.BlockManager, path string, buf []byte, blockSize int) error {
	bs := blockSize
	bm.Block_idx = 0
	for len(buf) > 0 {
		n := bs
		if len(buf) < bs {
			n = len(buf)
		}
		if err := bm.WriteBlock(path, buf[:n]); err != nil {
			return err
		}
		buf = buf[n:]
	}
	return nil
}

// readSegment vraća tačno "length" bajtova počev od "offset" u fajlu.
func readSegment(bm *blockmanager.BlockManager, path string, offset int64, length int, blockSize int) ([]byte, error) {
	bs := blockSize
	startBlk := int(offset / int64(bs))
	endBlk := int((offset + int64(length-1)) / int64(bs))

	out := make([]byte, length)
	pos := 0
	for blk := startBlk; blk <= endBlk; blk++ {
		block, err := bm.ReadBlock(path, blk)
		if err != nil {
			return nil, err
		}
		blkStart := 0
		if blk == startBlk {
			blkStart = int(offset) % bs
		}
		blkEnd := bs
		if blk == endBlk {
			blkEnd = (int(offset) + length) % bs
			if blkEnd == 0 {
				blkEnd = bs
			}
		}
		copy(out[pos:], block[blkStart:blkEnd])
		pos += blkEnd - blkStart
	}
	return out, nil
}

// calculateCRC računa CRC32 (IEEE) preko svih polja osim samog CRC-a.
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

// recordBytes serijalizuje Record u binarni format identičan WAL zapisu.
func recordBytes(r Record) []byte {
	body := make([]byte, 0, 37+len(r.Key)+len(r.Value))
	body = append(body, r.Timestamp[:]...)
	if r.Tombstone {
		body = append(body, 1)
	} else {
		body = append(body, 0)
	}
	body = binary.LittleEndian.AppendUint64(body, r.KeySize)
	body = binary.LittleEndian.AppendUint64(body, r.ValueSize)
	body = append(body, r.Key...)
	body = append(body, r.Value...)
	r.CRC = crc32.ChecksumIEEE(body)
	out := binary.LittleEndian.AppendUint32([]byte{}, r.CRC)
	return append(out, body...)
}

// CreateSSTable formira Data, Index, Summary, Filter i Metadata fajlove.
// - records  : sortirani niz zapisa koji se flush-uje iz mem-tabele
// - dir      : gde smestiti sve fajlove
// - step     : razmak (u broju zapisa) između dva unosa u Summary-ju
// - bm       : globalni BlockManager
// Funkcija vraća *SSTable sa popunjenim BloomFilter-om i MerkleTree-om.
func CreateSSTable(records []Record, dir string, step int, bm *blockmanager.BlockManager, blockSize int, singleFile bool) (*SSTable, error) {
	if len(records) == 0 {
		return nil, errors.New("no records to create SSTable")
	}

	if singleFile {
		return createSingleFileSSTable(records, dir, step, bm, blockSize)
	}
	return createMultiFileSSTable(records, dir, step, bm, blockSize)
}

// createMultiFileSSTable kreira SSTable u više fajlova koristeći BlockManager.
func createMultiFileSSTable(records []Record, dir string, step int, bm *blockmanager.BlockManager, blockSize int) (*SSTable, error) {
	bs := blockSize
	bloom := probabilistic.CreateBF(len(records), 0.01)

	dataBlk := make([]byte, 0, bs)
	indexBlk := make([]byte, 0, bs)
	leaves := make([][]byte, 0)

	offset := uint64(0)
	summaryEntries := make([]SummaryEntry, 0)

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	sst := NewMultiFileSSTable(dir)

	for i, rec := range records {
		rec.KeySize = uint64(len(rec.Key))
		rec.ValueSize = uint64(len(rec.Value))
		rec.CRC = calculateCRC(rec)
		rb := recordBytes(rec)

		if len(dataBlk)+len(rb) > bs {
			pad := make([]byte, bs-len(dataBlk))
			dataBlk = append(dataBlk, pad...)
			if err := bm.WriteBlock(sst.DataFilePath, dataBlk); err != nil {
				return nil, err
			}
			h := md5.Sum(dataBlk)
			leaves = append(leaves, h[:])
			dataBlk = make([]byte, 0, bs)
		}
		dataBlk = append(dataBlk, rb...)

		idxEntry := make([]byte, 0, 8+len(rec.Key)+8)
		idxEntry = binary.LittleEndian.AppendUint64(idxEntry, rec.KeySize)
		idxEntry = append(idxEntry, rec.Key...)
		idxEntry = binary.LittleEndian.AppendUint64(idxEntry, offset)
		if len(indexBlk)+len(idxEntry) > bs {
			pad := make([]byte, bs-len(indexBlk))
			indexBlk = append(indexBlk, pad...)
			bm.Block_idx = 0
			if err := bm.WriteBlock(sst.IndexFilePath, indexBlk); err != nil {
				return nil, err
			}
			indexBlk = make([]byte, 0, bs)
		}
		indexBlk = append(indexBlk, idxEntry...)

		bloom.AddElement(string(rec.Key))
		if i%step == 0 {
			summaryEntries = append(summaryEntries, SummaryEntry{Key: rec.Key, Offset: offset})
		}
		offset += uint64(len(rb))
	}

	if len(dataBlk) > 0 {
		pad := make([]byte, bs-len(dataBlk))
		dataBlk = append(dataBlk, pad...)
		_ = bm.WriteBlock(sst.DataFilePath, dataBlk)
		h := md5.Sum(dataBlk)
		leaves = append(leaves, h[:])
	}
	if len(indexBlk) > 0 {
		bm.Block_idx = 0
		pad := make([]byte, bs-len(indexBlk))
		indexBlk = append(indexBlk, pad...)
		_ = bm.WriteBlock(sst.IndexFilePath, indexBlk)
	}

	sum := make([]byte, 0)
	minK := records[0].Key
	maxK := records[len(records)-1].Key
	sum = binary.LittleEndian.AppendUint64(sum, uint64(len(minK)))
	sum = append(sum, minK...)
	sum = binary.LittleEndian.AppendUint64(sum, uint64(len(maxK)))
	sum = append(sum, maxK...)
	sum = binary.LittleEndian.AppendUint64(sum, uint64(len(summaryEntries)))
	for _, se := range summaryEntries {
		sum = binary.LittleEndian.AppendUint64(sum, uint64(len(se.Key)))
		sum = append(sum, se.Key...)
		sum = binary.LittleEndian.AppendUint64(sum, se.Offset)
	}
	_ = writeBlocks(bm, sst.SummaryFilePath, sum, blockSize)

	_ = writeBlocks(bm, sst.FilterFilePath, bloom.Serialize(), blockSize)

	leavesBytes := make([]byte, 0, len(leaves)*16)
	for _, h := range leaves {
		leavesBytes = append(leavesBytes, h...)
	}
	mt := merkletree.NewMerkleTree()
	mt.ConstructMerkleTree(leavesBytes, 16)
	_ = writeBlocks(bm, sst.MetadataFilePath, mt.Serialize(), blockSize)

	sst.Filter = &bloom
	sst.Metadata = &mt
	return sst, nil
}

// createSingleFileSSTable kreira SSTable u jednom fajlu koristeci BlockManager.
func createSingleFileSSTable(records []Record, dir string, step int, bm *blockmanager.BlockManager, blockSize int) (*SSTable, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	sst := NewSingleFileSSTable(dir)
	b := &bytes.Buffer{}
	bloom := probabilistic.CreateBF(len(records), 0.01)
	offsetMap := make([]int64, 5)

	dataBuf := &bytes.Buffer{}
	leaves := make([][]byte, 0)
	for _, rec := range records {
		rec.KeySize = uint64(len(rec.Key))
		rec.ValueSize = uint64(len(rec.Value))
		rec.CRC = calculateCRC(rec)
		rb := recordBytes(rec)
		dataBuf.Write(rb)
		h := md5.Sum(rb)
		leaves = append(leaves, h[:])
		bloom.AddElement(string(rec.Key))
	}
	offsetMap[0] = int64(b.Len())
	b.Write(dataBuf.Bytes())

	indexBuf := &bytes.Buffer{}
	offset := uint64(0)
	for _, rec := range records {
		binary.Write(indexBuf, binary.LittleEndian, rec.KeySize)
		indexBuf.Write(rec.Key)
		binary.Write(indexBuf, binary.LittleEndian, offset)
		offset += uint64(len(recordBytes(rec)))
	}
	offsetMap[1] = int64(b.Len())
	b.Write(indexBuf.Bytes())

	summaryBuf := &bytes.Buffer{}
	minK := records[0].Key
	maxK := records[len(records)-1].Key
	binary.Write(summaryBuf, binary.LittleEndian, uint64(len(minK)))
	summaryBuf.Write(minK)
	binary.Write(summaryBuf, binary.LittleEndian, uint64(len(maxK)))
	summaryBuf.Write(maxK)
	count := (len(records) + step - 1) / step
	binary.Write(summaryBuf, binary.LittleEndian, uint64(count))
	for i := 0; i < len(records); i += step {
		rec := records[i]
		binary.Write(summaryBuf, binary.LittleEndian, uint64(len(rec.Key)))
		summaryBuf.Write(rec.Key)
		off := uint64(0)
		for j := 0; j < i; j++ {
			off += uint64(len(recordBytes(records[j])))
		}
		binary.Write(summaryBuf, binary.LittleEndian, off)
	}
	offsetMap[2] = int64(b.Len())
	b.Write(summaryBuf.Bytes())

	filterBytes := bloom.Serialize()
	offsetMap[3] = int64(b.Len())
	b.Write(filterBytes)

	leavesBytes := make([]byte, 0, len(leaves)*16)
	for _, h := range leaves {
		leavesBytes = append(leavesBytes, h...)
	}
	mt := merkletree.NewMerkleTree()
	mt.ConstructMerkleTree(leavesBytes, 16)
	metadata := mt.Serialize()
	offsetMap[4] = int64(b.Len())
	b.Write(metadata)

	footer := &bytes.Buffer{}
	for _, off := range offsetMap {
		binary.Write(footer, binary.LittleEndian, uint64(off))
	}
	b.Write(footer.Bytes())

	bm.Block_idx = 0
	if err := writeBlocks(bm, sst.SingleFilePath, b.Bytes(), blockSize); err != nil {
		return nil, err
	}

	sst.Filter = &bloom
	sst.Metadata = &mt
	return sst, nil
}

// ReadRecordAtOffset čita kompletan Record iz Data fajla počevši od zadatog offseta.
func ReadRecordAtOffset(bm *blockmanager.BlockManager, path string, offs int64, blockSize int) (*Record, int, error) {
	// prvo učitamo fiksni header (CRC+meta = 37 B)
	header, err := readSegment(bm, path, offs, 37, blockSize)
	if err != nil {
		return nil, 0, err
	}
	rdr := bytes.NewReader(header)

	rec := &Record{}
	binary.Read(rdr, binary.LittleEndian, &rec.CRC)
	rdr.Read(rec.Timestamp[:])
	binary.Read(rdr, binary.LittleEndian, &rec.Tombstone)
	binary.Read(rdr, binary.LittleEndian, &rec.KeySize)
	binary.Read(rdr, binary.LittleEndian, &rec.ValueSize)

	total := 37 + int(rec.KeySize) + int(rec.ValueSize)
	full, err := readSegment(bm, path, offs, total, blockSize)
	if err != nil {
		return nil, total, err
	}
	rec.Key = append([]byte{}, full[37:37+rec.KeySize]...)
	rec.Value = append(rec.Value, full[37+rec.KeySize:]...)

	if calculateCRC(*rec) != rec.CRC {
		return nil, total, errors.New("CRC mismatch – corrupted record")
	}
	return rec, total, nil
}

// ReadIndexBlock čita jedan blok (fixed size) iz Index fajla i parsira sve unose.
func ReadIndexBlock(bm *blockmanager.BlockManager, path string, blkSize int64, offs int64, blockSize int) ([]Index, error) {
	buf, err := readSegment(bm, path, offs, int(blkSize), blockSize)
	if err != nil {
		return nil, err
	}
	rdr := bytes.NewReader(buf)
	var idxs []Index
	for rdr.Len() > 0 {
		var ksz uint64
		if err := binary.Read(rdr, binary.LittleEndian, &ksz); err != nil {
			break
		}
		key := make([]byte, ksz)
		if _, err := io.ReadFull(rdr, key); err != nil {
			break
		}
		var off uint64
		if err := binary.Read(rdr, binary.LittleEndian, &off); err != nil {
			break
		}
		idxs = append(idxs, Index{Key: key, Offset: off})
	}
	return idxs, nil
}

// LoadSummary stream-parsirа Summary fajl bez učitavanja celokupnog sadržaja u RAM.
func LoadSummary(bm *blockmanager.BlockManager, path string) (Summary, error) {
	blk, err := bm.ReadBlock(path, 0)
	if err != nil {
		return Summary{}, err
	}
	rdr := bytes.NewReader(blk)

	var minSz, maxSz, cnt uint64
	binary.Read(rdr, binary.LittleEndian, &minSz)
	minK := make([]byte, minSz)
	io.ReadFull(rdr, minK)
	binary.Read(rdr, binary.LittleEndian, &maxSz)
	maxK := make([]byte, maxSz)
	io.ReadFull(rdr, maxK)
	binary.Read(rdr, binary.LittleEndian, &cnt)

	entries := make([]SummaryEntry, 0, cnt)
	buf := blk[len(blk)-rdr.Len():]
	blkIdx := 1
	for uint64(len(entries)) < cnt {
		for len(buf) < 17 {
			nxt, err := bm.ReadBlock(path, blkIdx)
			if err != nil {
				return Summary{}, err
			}
			blkIdx++
			buf = append(buf, nxt...)
		}
		ksz := binary.LittleEndian.Uint64(buf[:8])
		need := 8 + int(ksz) + 8
		for len(buf) < need {
			nxt, _ := bm.ReadBlock(path, blkIdx)
			blkIdx++
			buf = append(buf, nxt...)
		}
		key := append([]byte{}, buf[8:8+ksz]...)
		off := binary.LittleEndian.Uint64(buf[8+ksz : need])
		entries = append(entries, SummaryEntry{Key: key, Offset: off})
		buf = buf[need:]
	}
	return Summary{MinKey: minK, MaxKey: maxK, Entries: entries}, nil
}

// LoadBloomFilter učitava Bloom filter iz fajla (fajl je obično mali).
func LoadBloomFilter(_ *blockmanager.BlockManager, path string) (*probabilistic.BloomFilter, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	bf := &probabilistic.BloomFilter{}
	if err := bf.Deserialize(file); err != nil {
		return nil, err
	}
	return bf, nil
}

// LoadMerkleTree deserializuje Merkle stablo sa diska.
func LoadMerkleTree(_ *blockmanager.BlockManager, path string) (*merkletree.MerkleTree, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	mt := merkletree.NewMerkleTree()
	if err := mt.Deserialize(f); err != nil {
		return nil, err
	}
	return &mt, nil
}

// ValidateMerkleTree ponovo hashira svaki data-blok i poredi sa upisanim stablom.
// Vraća false i indeks prvog izmenjenog bloka ukoliko se detektuje nepodudaranje.
func ValidateMerkleTree(bm *blockmanager.BlockManager, sst *SSTable, blockSize int) (bool, error) {
	bs := blockSize
	hashes := make([][]byte, 0)

	for blkIdx := 0; ; blkIdx++ {
		blk, err := bm.ReadBlock(sst.DataFilePath, blkIdx)
		if err != nil {
			return false, err
		}
		fi, _ := os.Stat(sst.DataFilePath)
		if int64(blkIdx*bs) >= fi.Size() {
			break
		}
		if int64((blkIdx+1)*bs) > fi.Size() {
			blk = blk[:fi.Size()-int64(blkIdx*bs)]
		}
		h := md5.Sum(blk)
		hashes = append(hashes, h[:])
		if int64((blkIdx+1)*bs) >= fi.Size() {
			break
		}
	}

	concat := make([]byte, 0, len(hashes)*16)
	for _, h := range hashes {
		concat = append(concat, h...)
	}
	tmp := merkletree.NewMerkleTree()
	tmp.ConstructMerkleTree(concat, 16)

	ok, diff := merkletree.Compare(&tmp, sst.Metadata)
	if ok {
		return true, nil
	}
	return false, fmt.Errorf("promena detektovana – prvi neispravan blok indeks %d", diff)
}

// FindIndexBlockOffset traži offset Index bloka u Summary-ju za dati ključ.
func FindIndexBlockOffset(summary Summary, key []byte) int64 {
	for i := len(summary.Entries) - 1; i >= 0; i-- {
		if bytes.Compare(summary.Entries[i].Key, key) <= 0 {
			return int64(summary.Entries[i].Offset)
		}
	}
	return 0
}

// Search sprovodi standardni Bloom → Summary → Index → Data redosled.
func Search(bm *blockmanager.BlockManager, sst *SSTable, key []byte, summary Summary, idxBlkSize int64, blockSize int) (*Record, error) {
	if !sst.Filter.IsAdded(string(key)) {
		return nil, fmt.Errorf("key not found (Bloom filter)")
	}
	if bytes.Compare(key, summary.MinKey) < 0 || bytes.Compare(key, summary.MaxKey) > 0 {
		return nil, fmt.Errorf("key outside summary range")
	}

	idxOff := FindIndexBlockOffset(summary, key)
	indices, err := ReadIndexBlock(bm, sst.IndexFilePath, idxBlkSize, idxOff, blockSize)
	if err != nil {
		return nil, err
	}

	var dataOff uint64
	found := false
	for _, idx := range indices {
		if bytes.Equal(idx.Key, key) {
			dataOff = idx.Offset
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("key not found in index")
	}
	read, _, err := ReadRecordAtOffset(bm, sst.DataFilePath, int64(dataOff), blockSize)
	return read, err
}

// parseFooter čita footer iz SSTable fajla i vraća offsete za Summary, Index, Bloom filter i Metadata.
func parseFooter(bm *blockmanager.BlockManager, path string, blockSize int) ([]int64, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	size := fi.Size()
	footerSize := 5 * 8
	buf, err := readSegment(bm, path, size-int64(footerSize), footerSize, blockSize)
	if err != nil {
		return nil, err
	}
	offsets := make([]int64, 5)
	for i := 0; i < 5; i++ {
		offsets[i] = int64(binary.LittleEndian.Uint64(buf[i*8 : (i+1)*8]))
	}
	return offsets, nil
}

// LoadSummarySingleFile učitava Summary iz jednog SSTable fajla.
func LoadSummarySingleFile(bm *blockmanager.BlockManager, path string, blockSize int, summaryOffset int64) (Summary, error) {
	buf, err := readSegment(bm, path, summaryOffset, 4096, blockSize) // conservative max summary size
	if err != nil {
		return Summary{}, err
	}
	rdr := bytes.NewReader(buf)

	var minSz, maxSz, cnt uint64
	binary.Read(rdr, binary.LittleEndian, &minSz)
	minK := make([]byte, minSz)
	io.ReadFull(rdr, minK)
	binary.Read(rdr, binary.LittleEndian, &maxSz)
	maxK := make([]byte, maxSz)
	io.ReadFull(rdr, maxK)
	binary.Read(rdr, binary.LittleEndian, &cnt)

	entries := make([]SummaryEntry, 0, cnt)
	for i := 0; i < int(cnt); i++ {
		var ksz uint64
		binary.Read(rdr, binary.LittleEndian, &ksz)
		key := make([]byte, ksz)
		io.ReadFull(rdr, key)
		var off uint64
		binary.Read(rdr, binary.LittleEndian, &off)
		entries = append(entries, SummaryEntry{Key: key, Offset: off})
	}
	return Summary{MinKey: minK, MaxKey: maxK, Entries: entries}, nil
}

// LoadBloomFilterSingleFile učitava Bloom filter iz jednog SSTable fajla.
// func LoadBloomFilterSingleFile(bm *blockmanager.BlockManager, path string, blockSize int, filterOffset int64, nextOffset int64) (*probabilistic.BloomFilter, error) {
// 	length := nextOffset - filterOffset
// 	buf, err := readSegment(bm, path, filterOffset, int(length), blockSize)
// 	if err != nil {
// 		return nil, err
// 	}
// 	bf := &probabilistic.BloomFilter{}
// 	err = bf.Deserialize(bytes.NewReader(buf))
// 	return bf, err
// }

// LoadMerkleTreeSingleFile učitava Merkle stablo iz jednog SSTable fajla.
// func LoadMerkleTreeSingleFile(bm *blockmanager.BlockManager, path string, blockSize int, metadataOffset int64, footerOffset int64) (*merkletree.MerkleTree, error) {
// 	length := footerOffset - metadataOffset
// 	buf, err := readSegment(bm, path, metadataOffset, int(length), blockSize)
// 	if err != nil {
// 		return nil, err
// 	}
// 	mt := merkletree.NewMerkleTree()
// 	err = mt.Deserialize(bytes.NewReader(buf))
// 	return mt, err
// }

// ReadRecordAtOffsetSingleFile čita Record iz jednog SSTable fajla na osnovu offseta.
func ReadRecordAtOffsetSingleFile(bm *blockmanager.BlockManager, path string, offset int64, blockSize int) (*Record, int, error) {
	header, err := readSegment(bm, path, offset, 37, blockSize)
	if err != nil {
		return nil, 0, err
	}
	rdr := bytes.NewReader(header)
	rec := &Record{}
	binary.Read(rdr, binary.LittleEndian, &rec.CRC)
	rdr.Read(rec.Timestamp[:])
	binary.Read(rdr, binary.LittleEndian, &rec.Tombstone)
	binary.Read(rdr, binary.LittleEndian, &rec.KeySize)
	binary.Read(rdr, binary.LittleEndian, &rec.ValueSize)
	total := 37 + int(rec.KeySize) + int(rec.ValueSize)
	full, err := readSegment(bm, path, offset, total, blockSize)
	if err != nil {
		return nil, total, err
	}
	rec.Key = append([]byte{}, full[37:37+rec.KeySize]...)
	rec.Value = append(rec.Value, full[37+rec.KeySize:]...)

	if calculateCRC(*rec) != rec.CRC {
		return nil, total, errors.New("CRC mismatch – corrupted record")
	}
	return rec, total, nil
}

// ReadIndexBlockSingleFile čita Index blok iz fajla u jednom SSTable formatu.
func ReadIndexBlockSingleFile(bm *blockmanager.BlockManager, path string, indexOffset int64, blockSize int) ([]Index, error) {
	buf, err := readSegment(bm, path, indexOffset, 4096, blockSize)
	if err != nil {
		return nil, err
	}
	rdr := bytes.NewReader(buf)
	var idxs []Index
	for rdr.Len() > 0 {
		var ksz uint64
		if err := binary.Read(rdr, binary.LittleEndian, &ksz); err != nil {
			break
		}
		key := make([]byte, ksz)
		if _, err := io.ReadFull(rdr, key); err != nil {
			break
		}
		var off uint64
		if err := binary.Read(rdr, binary.LittleEndian, &off); err != nil {
			break
		}
		idxs = append(idxs, Index{Key: key, Offset: off})
	}
	return idxs, nil
}

// SearchSingleFile sprovodi standardni Bloom → Summary → Index → Data redosled za SSTable u jednom fajlu.
// func SearchSingleFile(bm *blockmanager.BlockManager, sst *SSTable, key []byte, blockSize int) (*Record, error) {
// 	offsets, err := parseFooter(bm, sst.SingleFilePath, blockSize)
// 	if err != nil {
// 		return nil, err
// 	}

// 	filter, err := LoadBloomFilterSingleFile(bm, sst.SingleFilePath, blockSize, offsets[3], offsets[4])
// 	if err != nil || !filter.IsAdded(string(key)) {
// 		return nil, fmt.Errorf("key not found (Bloom filter)")
// 	}

// 	summary, err := LoadSummarySingleFile(bm, sst.SingleFilePath, blockSize, offsets[2])
// 	if err != nil {
// 		return nil, err
// 	}

// 	if bytes.Compare(key, summary.MinKey) < 0 || bytes.Compare(key, summary.MaxKey) > 0 {
// 		return nil, fmt.Errorf("key outside summary range")
// 	}

// 	idxOff := FindIndexBlockOffset(summary, key)
// 	indices, err := ReadIndexBlockSingleFile(bm, sst.SingleFilePath, offsets[1]+idxOff, blockSize)
// 	if err != nil {
// 		return nil, err
// 	}

// 	var dataOff uint64
// 	found := false
// 	for _, idx := range indices {
// 		if bytes.Equal(idx.Key, key) {
// 			dataOff = idx.Offset
// 			found = true
// 			break
// 		}
// 	}
// 	if !found {
// 		return nil, fmt.Errorf("key not found in index")
// 	}
// 	rec, _, err := ReadRecordAtOffsetSingleFile(bm, sst.SingleFilePath, int64(dataOff), blockSize)
// 	return rec, err
// }
