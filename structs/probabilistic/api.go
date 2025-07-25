package probabilistic

import (
	"encoding/binary"
	"errors"
)

// interfejs koristimo samo u load fjama, za ucitavaje iz memtable i sstable
// add pise u memtable koristeci timestamp
type SimpleKV interface {
	Add(ts [16]byte, tombstone bool, key string, value []byte) error
	Get(key string) ([]byte, bool, bool)
}

// interfejs za pun write path append je za wal, vraca timestamp
type FullKV interface {
	Add(ts [16]byte, tombstone bool, key string, value []byte) error
	Append(tombstone bool, key string, value []byte) ([16]byte, error)
	Get(key string) ([]byte, bool, bool)
}

func internalKey(prefix, name string) string {
	return "__sys__prob__" + prefix + "__" + name
}

//// ========== BLOOM FILTER ==========

func SaveBloom(name string, bf *BloomFilter, kv FullKV) error {
	key := internalKey("bf", name)
	value := bf.Serialize()
	ts, err := kv.Append(false, key, value) //zapisem u wal
	if err != nil {
		return err
	}
	return kv.Add(ts, false, key, value) // tek onda memtable
}

func LoadBloom(name string, mt SimpleKV) (*BloomFilter, error) {
	key := internalKey("bf", name)
	data, del, ok := mt.Get(key)
	if !ok || del {
		return nil, errors.New("ne postoji BloomFilter sa tim imenom")
	}
	bf := &BloomFilter{}
	bf.Deserialize(data)
	return bf, nil
}

//// ========== COUNT MIN SKETCH ==========

func SaveCMS(name string, cms *CountMinSketch, kv FullKV) error {
	key := internalKey("cms", name)
	value := cms.Serialize()
	ts, err := kv.Append(false, key, value)
	if err != nil {
		return err
	}
	return kv.Add(ts, false, key, value)
}

func LoadCMS(name string, mt SimpleKV) (*CountMinSketch, error) {
	key := internalKey("cms", name)
	data, del, ok := mt.Get(key)
	if !ok || del {
		return nil, errors.New("ne postoji CountMinSketch sa tim imenom")
	}
	cms := &CountMinSketch{}
	err := cms.DeserializeFromBytes(data)
	return cms, err
}

func (cms *CountMinSketch) DeserializeFromBytes(data []byte) error {
	k := binary.BigEndian.Uint32(data[:4])
	m := binary.BigEndian.Uint32(data[4:8])
	offset := 8
	matrix := make([][]uint32, k)
	for i := 0; i < int(k); i++ {
		matrix[i] = make([]uint32, m)
		for j := 0; j < int(m); j++ {
			matrix[i][j] = binary.BigEndian.Uint32(data[offset : offset+4])
			offset += 4
		}
	}
	n := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4
	hashes := make([]HashWithSeed, n)
	for i := 0; i < int(n); i++ {
		hashes[i] = HashWithSeed{Seed: data[offset : offset+32]}
		offset += 32
	}
	cms.Matrix = matrix
	cms.HashFunctions = hashes
	return nil
}

//// ========== HYPERLOGLOG ==========

func SaveHLL(name string, hll *HyperLogLog, kv FullKV) error {
	key := internalKey("hll", name)
	value := hll.Serialize()
	ts, err := kv.Append(false, key, value)
	if err != nil {
		return err
	}
	return kv.Add(ts, false, key, value)
}

func LoadHLL(name string, mt SimpleKV) (*HyperLogLog, error) {
	key := internalKey("hll", name)
	data, del, ok := mt.Get(key)
	if !ok || del {
		return nil, errors.New("ne postoji HyperLogLog sa tim imenom")
	}
	hll := &HyperLogLog{}
	err := hll.Deserialize(data)
	return hll, err
}

func (hll *HyperLogLog) Serialize() []byte {
	bytes := make([]byte, 0)
	bytes = append(bytes, hll.p)
	bytes = append(bytes, hll.reg...)
	return bytes
}

func (hll *HyperLogLog) Deserialize(data []byte) error {
	hll.p = data[0]
	hll.m = 1 << hll.p
	hll.reg = make([]uint8, hll.m)
	copy(hll.reg, data[1:])
	return nil
}

//// ========== SIMHASH ==========

func SaveSimhash(name string, sim uint64, kv FullKV) error {
	key := internalKey("sim", name)
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, sim)
	ts, err := kv.Append(false, key, value)
	if err != nil {
		return err
	}
	return kv.Add(ts, false, key, value)
}

func LoadSimhash(name string, mt SimpleKV) (uint64, error) {
	key := internalKey("sim", name)
	data, del, ok := mt.Get(key)
	if !ok || del {
		return 0, errors.New("ne postoji SimHash sa tim imenom")
	}
	return binary.BigEndian.Uint64(data), nil
}
