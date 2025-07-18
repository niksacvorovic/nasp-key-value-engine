package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"time"

	"projekat/config"
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
	Compaction byte
	MinKey     []byte
	MaxKey     []byte
	Entries    []SummaryEntry
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
func NewSingleFileSSTable(path string, ts int64) *SSTable {
	return &SSTable{
		SingleFilePath: filepath.Join(path, fmt.Sprintf("%d-SSTable.db", ts)),
		SingleSSTable:  true,
	}
}

func NewMultiFileSSTable(path string, ts int64) *SSTable {
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

// WriteUvarint upisuje uint64 vrednost koristeci varijabilni enkoding
func WriteUvarint(buf *bytes.Buffer, val uint64) {
	b := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(b, val)
	buf.Write(b[:n])
}

// ReadUvarint cita uint64 vrednost sa varijabilnim enkodingom
func ReadUvarint(r io.ByteReader) (uint64, error) {
	val, err := binary.ReadUvarint(r)
	if err != nil {
		return 0, errors.New("failed to read uvarint: " + err.Error())
	}
	return val, nil
}

// recordBytes serijalizuje Record u binarni format identičan WAL zapisu.
func recordBytes(r Record, keyId uint64, compress bool) []byte {
	buf := &bytes.Buffer{}

	if compress {
		// Rezerviši prostor za CRC (4B)
		buf.Write(make([]byte, 4))

		// Timestamp (16B)
		buf.Write(r.Timestamp[:])

		// Tombstone (1B)
		if r.Tombstone {
			buf.WriteByte(1)
			WriteUvarint(buf, keyId) // Samo ID ključa
		} else {
			buf.WriteByte(0)
			WriteUvarint(buf, keyId)                // ID ključa
			WriteUvarint(buf, uint64(len(r.Value))) // Varint dužina vrednosti
			buf.Write(r.Value)                      // Vrednost
		}

		// Računanje i upis CRC
		data := buf.Bytes()
		crc := crc32.ChecksumIEEE(data[4:]) // Bez CRC polja
		binary.LittleEndian.PutUint32(data[0:4], crc)
		return data

	} else {
		// Ne-kompresovani slučaj
		r.KeySize = uint64(len(r.Key))
		r.ValueSize = uint64(len(r.Value))

		buf.Write(r.Timestamp[:])
		if r.Tombstone {
			buf.WriteByte(1)
		} else {
			buf.WriteByte(0)
		}
		binary.Write(buf, binary.LittleEndian, r.KeySize)
		binary.Write(buf, binary.LittleEndian, r.ValueSize)
		buf.Write(r.Key)
		buf.Write(r.Value)

		r.CRC = crc32.ChecksumIEEE(buf.Bytes())
		out := binary.LittleEndian.AppendUint32([]byte{}, r.CRC)
		return append(out, buf.Bytes()...)
	}
}

// CreateSSTable formira Data, Index, Summary, Filter i Metadata fajlove.
// - records  : sortirani niz zapisa koji se flush-uje iz mem-tabele
// - dir      : gde smestiti sve fajlove
// - step     : razmak (u broju zapisa) između dva unosa u Summary-ju
// - bm       : globalni BlockManager
// Funkcija vraća *SSTable sa popunjenim BloomFilter-om i MerkleTree-om.
func CreateSSTable(records []Record, dir string, step int, bm *blockmanager.BlockManager, blockSize int,
	lsm byte, singleFile bool, compress bool, dict *Dictionary, dictPath string) (*SSTable, string, error) {
	if len(records) == 0 {
		return nil, "", errors.New("no records to create SSTable")
	}

	if singleFile {
		return createSingleFileSSTable(records, dir, step, bm, blockSize, lsm, compress, dict, dictPath)
	}
	return createMultiFileSSTable(records, dir, step, bm, blockSize, lsm, compress, dict, dictPath)
}

// createMultiFileSSTable kreira SSTable u više fajlova koristeći BlockManager.
func createMultiFileSSTable(records []Record, dir string, step int, bm *blockmanager.BlockManager, blockSize int,
	lsm byte, compress bool, dict *Dictionary, dictPath string) (*SSTable, string, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, "", err
	}

	timestamp := time.Now().UnixNano()
	sstDir := filepath.Join(dir, fmt.Sprintf("%d-sstable", timestamp))
	if err := os.MkdirAll(sstDir, 0755); err != nil {
		return nil, "", err
	}
	sst := NewMultiFileSSTable(sstDir, timestamp)

	bloom := probabilistic.CreateBF(len(records), 0.01)
	summaryEntries := make([]SummaryEntry, 0)

	dataBuf := &bytes.Buffer{}
	indexBuf := &bytes.Buffer{}

	for i, rec := range records {
		rec.KeySize = uint64(len(rec.Key))
		rec.ValueSize = uint64(len(rec.Value))
		rec.CRC = calculateCRC(rec)
		rb := recordBytes(rec, dict.GetID(string(rec.Key), bm, dictPath, blockSize), compress)

		// Zapis u dataBuf
		offsetNow := uint64(dataBuf.Len())
		dataBuf.Write(rb)

		// Zapis u indexBuf
		binary.Write(indexBuf, binary.LittleEndian, rec.KeySize)
		indexBuf.Write(rec.Key)
		binary.Write(indexBuf, binary.LittleEndian, offsetNow)

		// Bloom filter
		bloom.AddElement(string(rec.Key))

		// Summary
		if i%step == 0 {
			summaryEntries = append(summaryEntries, SummaryEntry{Key: rec.Key, Offset: offsetNow})
		}
	}
	if dataBuf.Len()%blockSize != 0 {
		padding := make([]byte, blockSize-(dataBuf.Len()/blockSize))
		dataBuf.Write(padding)
	}
	// Zapis data i index
	_ = writeBlocks(bm, sst.DataFilePath, dataBuf.Bytes(), blockSize)
	_ = writeBlocks(bm, sst.IndexFilePath, indexBuf.Bytes(), blockSize)

	// Summary
	sum := make([]byte, 0)
	sum = append(sum, lsm)
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

	// Filter
	_ = writeBlocks(bm, sst.FilterFilePath, bloom.Serialize(), blockSize)

	// Merkle
	mt := merkletree.NewMerkleTree()
	mt.ConstructMerkleTree(dataBuf.Bytes(), blockSize)
	_ = writeBlocks(bm, sst.MetadataFilePath, mt.Serialize(), blockSize)

	sst.Filter = &bloom
	sst.Metadata = &mt
	return sst, sstDir, nil
}

// createSingleFileSSTable kreira SSTable u jednom fajlu koristeci BlockManager.
func createSingleFileSSTable(records []Record, dir string, step int, bm *blockmanager.BlockManager, blockSize int,
	lsm byte, compress bool, dict *Dictionary, dictPath string) (*SSTable, string, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, "", err
	}

	timestamp := time.Now().UnixNano()
	sstDir := filepath.Join(dir, fmt.Sprintf("%d-sstable", timestamp))
	if err := os.MkdirAll(sstDir, 0755); err != nil {
		return nil, "", err
	}
	sst := NewSingleFileSSTable(sstDir, timestamp)
	var sstOffset int64 = 48
	b := &bytes.Buffer{}
	bloom := probabilistic.CreateBF(len(records), 0.01)
	offsetMap := make([]int64, 6)

	dataBuf := &bytes.Buffer{}
	for _, rec := range records {
		rec.KeySize = uint64(len(rec.Key))
		rec.ValueSize = uint64(len(rec.Value))
		rec.CRC = calculateCRC(rec)
		rb := recordBytes(rec, dict.GetID(string(rec.Key), bm, dictPath, blockSize), compress)
		dataBuf.Write(rb)
		bloom.AddElement(string(rec.Key))
	}
	offsetMap[0] = sstOffset
	sstOffset += int64(dataBuf.Len())
	b.Write(dataBuf.Bytes())

	indexBuf := &bytes.Buffer{}
	offset := uint64(0)
	for _, rec := range records {
		binary.Write(indexBuf, binary.LittleEndian, rec.KeySize)
		indexBuf.Write(rec.Key)
		binary.Write(indexBuf, binary.LittleEndian, offset)
		offset += uint64(len(recordBytes(rec, dict.GetID(string(rec.Key), bm, dictPath, blockSize), compress)))
	}
	offsetMap[1] = sstOffset
	sstOffset += int64(indexBuf.Len())
	b.Write(indexBuf.Bytes())

	summaryBuf := &bytes.Buffer{}
	binary.Write(summaryBuf, binary.LittleEndian, lsm)
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
			off += uint64(len(recordBytes(records[j], dict.GetID(string(records[j].Key), bm, dictPath, blockSize), compress)))
		}
		binary.Write(summaryBuf, binary.LittleEndian, off)
	}
	offsetMap[2] = sstOffset
	sstOffset += int64(summaryBuf.Len())
	b.Write(summaryBuf.Bytes())

	filterBytes := bloom.Serialize()
	offsetMap[3] = sstOffset
	sstOffset += int64(len(filterBytes))
	b.Write(filterBytes)

	mt := merkletree.NewMerkleTree()
	mt.ConstructMerkleTree(dataBuf.Bytes(), blockSize)
	metadata := mt.Serialize()
	offsetMap[4] = sstOffset
	sstOffset += int64(len(metadata))
	b.Write(metadata)
	// Upisivanje indeksa kraja sadržaja bloka
	offsetMap[5] = sstOffset

	header := &bytes.Buffer{}
	for _, off := range offsetMap {
		binary.Write(header, binary.LittleEndian, uint64(off))
	}

	bytesToWrite := append(header.Bytes(), b.Bytes()...)

	bm.Block_idx = 0
	if err := writeBlocks(bm, sst.SingleFilePath, bytesToWrite, blockSize); err != nil {
		return nil, "", err
	}

	sst.Filter = &bloom
	sst.Metadata = &mt
	return sst, sstDir, nil
}

// ReadRecordAtOffset čita kompletan Record iz Data fajla počevši od zadatog offseta.
func ReadRecordAtOffset(bm *blockmanager.BlockManager, path string, offs int64, blockSize int, compress bool, dict *Dictionary) (*Record, int, error) {
	if compress {
		header, err := readSegment(bm, path, offs, 21, blockSize) // CRC (4) + TS (16) + tomb (1)
		if err != nil {
			return nil, 0, err
		}
		rdr := bytes.NewReader(header)
		rec := &Record{}
		binary.Read(rdr, binary.LittleEndian, &rec.CRC)
		rdr.Read(rec.Timestamp[:])
		tomb, _ := rdr.ReadByte()
		rec.Tombstone = tomb == 1

		offset := offs + 21

		// Učitaj ostatak zapisa
		if rec.Tombstone {
			// Samo ID
			buf, _ := readSegment(bm, path, offset, 10, blockSize)
			r := bytes.NewReader(buf)
			keyId, _ := ReadUvarint(r)
			keyStr, err := dict.Lookup(keyId)
			if err != nil {
				return nil, 0, err
			}
			rec.Key = []byte(keyStr)
			totalLen := int(rdr.Size()) + r.Len()
			return rec, totalLen, nil
		} else {
			buf, _ := readSegment(bm, path, offset, 20, blockSize)
			r := bytes.NewReader(buf)
			keyId, _ := ReadUvarint(r)
			valSize, _ := ReadUvarint(r)
			valStart := offset + int64(r.Size()) - int64(r.Len())
			val, err := readSegment(bm, path, valStart, int(valSize), blockSize)
			if err != nil {
				return nil, 0, err
			}
			rec.Value = val
			keyStr, err := dict.Lookup(keyId)
			if err != nil {
				return nil, 0, err
			}
			rec.Key = []byte(keyStr)
			total := int(valStart + int64(valSize) - offs)
			// CRC provera
			if calculateCRC(*rec) != rec.CRC {
				return nil, total, errors.New("CRC mismatch – corrupted record")
			}
			return rec, total, nil
		}
	} else {
		// NE-kompresovani deo (ostaje isti)
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
	// Izbacivanje praznih zapisa nastalih zbog paddinga na bloku
	fullIndices := make([]Index, 0)
	for _, idx := range idxs {
		if len(idx.Key) != 0 {
			fullIndices = append(fullIndices, idx)
		}
	}
	return fullIndices, nil
}

// LoadSummary stream-parsirа Summary fajl bez učitavanja celokupnog sadržaja u RAM.
func LoadSummary(bm *blockmanager.BlockManager, path string) (Summary, error) {
	blk, err := bm.ReadBlock(path, 0)
	if err != nil {
		return Summary{}, err
	}
	rdr := bytes.NewReader(blk)

	var lsm byte
	var minSz, maxSz, cnt uint64
	binary.Read(rdr, binary.LittleEndian, &lsm)
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
	return Summary{Compaction: lsm, MinKey: minK, MaxKey: maxK, Entries: entries}, nil
}

// LoadBloomFilter učitava Bloom filter iz fajla (fajl je obično mali).
func LoadBloomFilter(bm *blockmanager.BlockManager, path string, blockSize int) (*probabilistic.BloomFilter, error) {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	bytes, err := readSegment(bm, path, 0, int(fileInfo.Size()), blockSize)
	if err != nil {
		return nil, err
	}
	bf := probabilistic.BloomFilter{}
	bf.Deserialize(bytes)
	return &bf, nil
}

// LoadMerkleTree deserializuje Merkle stablo sa diska.
func LoadMerkleTree(bm *blockmanager.BlockManager, path string, blockSize int) (*merkletree.MerkleTree, error) {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	bytes, err := readSegment(bm, path, 0, int(fileInfo.Size()), blockSize)
	if err != nil {
		return nil, err
	}

	mt := merkletree.NewMerkleTree()
	mt.Deserialize(bytes)
	return &mt, nil
}

// ValidateMerkleTree ponovo hashira svaki data-blok i poredi sa upisanim stablom.
// Vraća false i indeks prvog izmenjenog bloka ukoliko se detektuje nepodudaranje.
func ValidateMerkleTree(bm *blockmanager.BlockManager, sst *SSTable, blockSize int) (bool, error) {
	// bs := blockSize
	// hashes := make([][]byte, 0)

	// for blkIdx := 0; ; blkIdx++ {
	// 	blk, err := bm.ReadBlock(sst.DataFilePath, blkIdx)
	// 	if err != nil {
	// 		return false, err
	// 	}
	// 	fi, _ := os.Stat(sst.DataFilePath)
	// 	if int64(blkIdx*bs) >= fi.Size() {
	// 		break
	// 	}
	// 	if int64((blkIdx+1)*bs) > fi.Size() {
	// 		blk = blk[:fi.Size()-int64(blkIdx*bs)]
	// 	}
	// 	h := md5.Sum(blk)
	// 	hashes = append(hashes, h[:])
	// 	if int64((blkIdx+1)*bs) >= fi.Size() {
	// 		break
	// 	}
	// }

	// concat := make([]byte, 0, len(hashes)*16)
	// for _, h := range hashes {
	// 	concat = append(concat, h...)
	// }
	var err error
	var data []byte
	if sst.SingleSSTable {
		offsets, err := parseHeader(bm, sst.SingleFilePath, blockSize)
		if err != nil {
			return false, err
		}
		sst.Metadata, err = LoadMerkleTreeSingleFile(bm, sst.SingleFilePath, blockSize, offsets[4], offsets[5])
		if err != nil {
			return false, err
		}
		data, err = readSegment(bm, sst.SingleFilePath, 48, int(offsets[0])-48, blockSize)
		if err != nil {
			return false, err
		}
	} else {
		sst.Metadata, err = LoadMerkleTree(bm, sst.SingleFilePath, blockSize)
		if err != nil {
			return false, err
		}
		dataInfo, err := os.Stat(sst.DataFilePath)
		if err != nil {
			return false, err
		}
		data, err = readSegment(bm, sst.DataFilePath, 0, int(dataInfo.Size()), blockSize)
		if err != nil {
			return false, err
		}
	}
	tmp := merkletree.NewMerkleTree()
	tmp.ConstructMerkleTree(data, blockSize)

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
func Search(bm *blockmanager.BlockManager, sst *SSTable, key []byte, summary Summary,
	idxBlkSize int64, blockSize int, compress bool, dict *Dictionary) (*Record, int, error) {
	if !sst.Filter.IsAdded(string(key)) {
		return nil, 0, fmt.Errorf("key not found (Bloom filter)")
	}
	if bytes.Compare(key, summary.MinKey) < 0 || bytes.Compare(key, summary.MaxKey) > 0 {
		return nil, 0, fmt.Errorf("key outside summary range")
	}

	idxOff := FindIndexBlockOffset(summary, key)
	indices, err := ReadIndexBlock(bm, sst.IndexFilePath, idxBlkSize, idxOff, blockSize)
	if err != nil {
		return nil, 0, err
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
		return nil, 0, fmt.Errorf("key not found in index")
	}
	read, offset, err := ReadRecordAtOffset(bm, sst.DataFilePath, int64(dataOff), blockSize, compress, dict)
	return read, int(dataOff) + offset, err
}

// parseHeader čita header iz SSTable fajla i vraća offsete za Summary, Index,
// Bloom filter, Merkle stablo i kraj korisnih bajtova
func parseHeader(bm *blockmanager.BlockManager, path string, blockSize int) ([]int64, error) {
	// Header zauzima prvih 48 bajtova Single File SSTabele
	buf, err := readSegment(bm, path, 0, 48, blockSize)
	if err != nil {
		return nil, err
	}
	offsets := make([]int64, 6)
	for i := 0; i < 6; i++ {
		offsets[i] = int64(binary.LittleEndian.Uint64(buf[i*8 : (i+1)*8]))
	}
	return offsets, nil
}

// LoadSummarySingleFile učitava Summary iz jednog SSTable fajla.
func LoadSummarySingleFile(bm *blockmanager.BlockManager, path string, blockSize int, summaryOffset int64, nextOffset int64) (Summary, error) {
	length := nextOffset - summaryOffset
	buf, err := readSegment(bm, path, summaryOffset, int(length), blockSize) // conservative max summary size
	if err != nil {
		return Summary{}, err
	}
	rdr := bytes.NewReader(buf)

	var lsm byte
	var minSz, maxSz, cnt uint64
	binary.Read(rdr, binary.LittleEndian, &lsm)
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
	return Summary{Compaction: lsm, MinKey: minK, MaxKey: maxK, Entries: entries}, nil
}

// LoadBloomFilterSingleFile učitava Bloom filter iz jednog SSTable fajla.
func LoadBloomFilterSingleFile(bm *blockmanager.BlockManager, path string, blockSize int, filterOffset int64, nextOffset int64) (*probabilistic.BloomFilter, error) {
	length := nextOffset - filterOffset
	buf, err := readSegment(bm, path, filterOffset, int(length), blockSize)
	if err != nil {
		return nil, err
	}
	bf := &probabilistic.BloomFilter{}
	bf.Deserialize(buf)
	return bf, err
}

// LoadMerkleTreeSingleFile učitava Merkle stablo iz jednog SSTable fajla.
func LoadMerkleTreeSingleFile(bm *blockmanager.BlockManager, path string, blockSize int, metadataOffset int64, nextOffset int64) (*merkletree.MerkleTree, error) {
	length := nextOffset - metadataOffset
	buf, err := readSegment(bm, path, metadataOffset, int(length), blockSize)
	if err != nil {
		return nil, err
	}
	mt := merkletree.NewMerkleTree()
	mt.Deserialize(buf)
	return &mt, err
}

// ReadRecordAtOffsetSingleFile čita Record iz jednog SSTable fajla na osnovu offseta.
func ReadRecordAtOffsetSingleFile(bm *blockmanager.BlockManager, path string, offset int64, blockSize int,
	compress bool, dict *Dictionary) (*Record, int, error) {
	if compress {
		// Čitamo CRC (4) + Timestamp (16) + Tombstone (1)
		header, err := readSegment(bm, path, offset, 21, blockSize)
		if err != nil {
			return nil, 0, err
		}
		rdr := bytes.NewReader(header)
		rec := &Record{}
		binary.Read(rdr, binary.LittleEndian, &rec.CRC)
		rdr.Read(rec.Timestamp[:])
		tomb, _ := rdr.ReadByte()
		rec.Tombstone = tomb == 1

		currOffset := offset + 21

		if rec.Tombstone {
			// Samo ID ključa
			buf, _ := readSegment(bm, path, currOffset, 10, blockSize)
			r := bytes.NewReader(buf)
			keyId, _ := ReadUvarint(r)
			keyStr, err := dict.Lookup(keyId)
			if err != nil {
				return nil, 0, err
			}
			rec.Key = []byte(keyStr)
			totalLen := int(rdr.Size()) + r.Len()
			return rec, totalLen, nil
		} else {
			// ID + ValueSize + Value
			buf, _ := readSegment(bm, path, currOffset, 20, blockSize)
			r := bytes.NewReader(buf)
			keyId, _ := ReadUvarint(r)
			valSize, _ := ReadUvarint(r)
			valStart := currOffset + int64(r.Size()) - int64(r.Len())
			val, err := readSegment(bm, path, valStart, int(valSize), blockSize)
			if err != nil {
				return nil, 0, err
			}
			keyStr, err := dict.Lookup(keyId)
			if err != nil {
				return nil, 0, err
			}
			rec.Key = []byte(keyStr)
			rec.Value = val

			// CRC provera
			if calculateCRC(*rec) != rec.CRC {
				return nil, int(valStart + int64(len(val)) - offset), errors.New("CRC mismatch – corrupted record")
			}
			total := int(valStart + int64(len(val)) - offset)
			return rec, total, nil
		}
	} else {
		// NE-kompresovani slučaj (isto kao ranije)
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
func SearchSingleFile(bm *blockmanager.BlockManager, sst *SSTable, key []byte, blockSize int, compress bool, dict *Dictionary) (*Record, int, error) {
	offsets, err := parseHeader(bm, sst.SingleFilePath, blockSize)
	if err != nil {
		return nil, 0, err
	}

	filter, err := LoadBloomFilterSingleFile(bm, sst.SingleFilePath, blockSize, offsets[3], offsets[4])
	if err != nil || !filter.IsAdded(string(key)) {
		return nil, 0, fmt.Errorf("key not found (Bloom filter)")
	}

	summary, err := LoadSummarySingleFile(bm, sst.SingleFilePath, blockSize, offsets[2], offsets[3])
	if err != nil {
		return nil, 0, err
	}

	if bytes.Compare(key, summary.MinKey) < 0 || bytes.Compare(key, summary.MaxKey) > 0 {
		return nil, 0, fmt.Errorf("key outside summary range")
	}

	idxOff := FindIndexBlockOffset(summary, key)
	indices, err := ReadIndexBlockSingleFile(bm, sst.SingleFilePath, offsets[1]+idxOff, blockSize)
	if err != nil {
		return nil, 0, err
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
		return nil, 0, fmt.Errorf("key not found in index")
	}
	rec, offset, err := ReadRecordAtOffsetSingleFile(bm, sst.SingleFilePath, int64(dataOff), blockSize, compress, dict)
	return rec, int(dataOff) + offset, err
}

// SeatchSSTable je pomocna funkcija koja wrappuje SearchSingleFile i Search funkcije
func SearchSSTable(dir, key string, cfg config.Config, bm *blockmanager.BlockManager, compress bool, dict *Dictionary) (*Record, bool) {
	base := filepath.Base(dir)
	var ts int64
	if _, err := fmt.Sscanf(base, "%d-sstable", &ts); err != nil {
		return nil, false
	}

	var sst *SSTable
	if cfg.SSTableSingleFile {
		// Pretrazi po kljucu
		sst = NewSingleFileSSTable(filepath.Dir(dir), ts)
		sst.SingleFilePath = filepath.Join(dir, fmt.Sprintf("%d-SSTable.db", ts))

		record, _, err := SearchSingleFile(bm, sst, []byte(key), cfg.BlockSize, compress, dict)
		if err == nil {
			return record, true
		}
	} else {
		sst = NewMultiFileSSTable(dir, ts)

		// Ucitaj summary
		summary, err := LoadSummary(bm, sst.SummaryFilePath)
		if err != nil {
			return nil, false
		}

		// Ucitaj Bloom filter
		bloom, err := LoadBloomFilter(bm, sst.FilterFilePath, cfg.BlockSize)
		if err != nil {
			return nil, false
		}
		sst.Filter = bloom

		// Pretrazi po kljucu
		record, _, err := Search(bm, sst, []byte(key), summary, int64(cfg.BlockSize), cfg.BlockSize, compress, dict)
		if err == nil {
			return record, true
		}
	}
	return nil, false
}
