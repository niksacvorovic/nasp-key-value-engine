package probabilistic

import (
	"encoding/binary"
	"errors"
)

// interfejs koristi samo Add i Get, da ne uvozimo ceo memtable
// ovo je da se ne bi medjusobno ukljucivali
type SimpleKV interface {
	Add(ts [16]byte, tombstone bool, key string, value []byte) error
	Get(key string) ([]byte, bool, bool)
}

func internalKey(prefix, name string) string {
	return "__sys__prob__" + prefix + "__" + name
}

//// ========== BLOOM FILTER ==========

func SaveBloom(name string, bf *BloomFilter, mt SimpleKV) error {
	key := internalKey("bf", name)
	value := bf.Serialize()
	return mt.Add([16]byte{}, false, key, value)
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

func SaveCMS(name string, cms *CountMinSketch, mt SimpleKV) error {
	key := internalKey("cms", name)
	value := cms.Serialize()
	return mt.Add([16]byte{}, false, key, value)
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

func SaveHLL(name string, hll *HyperLogLog, mt SimpleKV) error {
	key := internalKey("hll", name)
	value := hll.Serialize()
	return mt.Add([16]byte{}, false, key, value)
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

func SaveSimhash(name string, sim uint64, mt SimpleKV) error {
	key := internalKey("sim", name)
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, sim)
	return mt.Add([16]byte{}, false, key, b)
}

func LoadSimhash(name string, mt SimpleKV) (uint64, error) {
	key := internalKey("sim", name)
	data, del, ok := mt.Get(key)
	if !ok || del {
		return 0, errors.New("ne postoji SimHash sa tim imenom")
	}
	return binary.BigEndian.Uint64(data), nil
}
