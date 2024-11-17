package structs

import (
	"crypto/md5"
	"encoding/binary"
	"math"
	"os"
	"time"
)

type HashWithSeed struct {
	Seed []byte
}

type BloomFilter struct {
	array  []bool
	hashes []HashWithSeed
}

func CalculateM(expectedElements int, falsePositiveRate float64) uint {
	return uint(math.Ceil(float64(expectedElements) * math.Abs(math.Log(falsePositiveRate)) / math.Pow(math.Log(2), float64(2))))
}

func CalculateK(expectedElements int, m uint) uint {
	return uint(math.Ceil((float64(m) / float64(expectedElements)) * math.Log(2)))
}

func (h HashWithSeed) Hash(data []byte) uint64 {
	fn := md5.New()
	fn.Write(append(data, h.Seed...))
	return binary.BigEndian.Uint64(fn.Sum(nil))
}

func CreateHashFunctions(k uint32) []HashWithSeed {
	h := make([]HashWithSeed, k)
	ts := uint32(time.Now().Unix())
	for i := uint32(0); i < k; i++ {
		seed := make([]byte, 4)
		binary.BigEndian.PutUint32(seed, ts+i)
		hfn := HashWithSeed{Seed: seed}
		h[i] = hfn
	}
	return h
}

func CreateBF(length int, rate float64) BloomFilter {
	m := CalculateM(length, rate)
	return BloomFilter{
		array:  make([]bool, m),
		hashes: CreateHashFunctions(uint32(CalculateK(length, m))),
	}
}

func (bf *BloomFilter) AddElement(elem string) {
	temp := []byte(elem)
	for i := 0; i < int(len(bf.hashes)); i++ {
		index := bf.hashes[i].Hash(temp)
		compressed := index % uint64(len(bf.array))
		bf.array[compressed] = true
	}
}

func (bf *BloomFilter) IsAdded(elem string) bool {
	temp := []byte(elem)
	for i := 0; i < len(bf.hashes); i++ {
		index := bf.hashes[i].Hash(temp)
		compressed := index % uint64(len(bf.array))
		if !bf.array[compressed] {
			return false
		}
	}
	return true
}

func (bf BloomFilter) Serialize() []byte {
	bytes := make([]byte, 0)
	m := uint32(len(bf.array))
	bytes = binary.BigEndian.AppendUint32(bytes, m)
	for i := 0; i < int(m); i++ {
		if bf.array[i] {
			bytes = append(bytes, 1)
		} else {
			bytes = append(bytes, 0)
		}
	}
	k := uint32(len(bf.hashes))
	bytes = binary.BigEndian.AppendUint32(bytes, k)
	for i := 0; i < int(k); i++ {
		bytes = append(bytes, bf.hashes[i].Seed...)
	}
	return bytes
}

func (bf *BloomFilter) Deserialize(file *os.File) error {
	// mbin - m binarno, kbin - k binarno
	mbin := make([]byte, 4)
	_, err := file.Read(mbin)
	if err != nil {
		panic(err)
	}
	m := binary.BigEndian.Uint32(mbin)
	fields := make([]byte, m)
	_, err = file.Read(fields)
	if err != nil {
		panic(err)
	}
	boolarr := make([]bool, m)
	for i := 0; i < int(m); i++ {
		if fields[i] == 1 {
			boolarr[i] = true
		} else {
			boolarr[i] = false
		}
	}
	bf.array = boolarr
	kbin := make([]byte, 4)
	_, err = file.Read(kbin)
	if err != nil {
		panic(err)
	}
	k := binary.BigEndian.Uint32(kbin)
	hasharr := make([]HashWithSeed, k)
	seedbuffer := make([]byte, 4)
	for i := 0; i < int(k); i++ {
		_, err = file.Read(seedbuffer)
		if err != nil {
			panic(err)
		}
		hash := HashWithSeed{Seed: seedbuffer}
		hasharr[i] = hash
	}
	bf.hashes = hasharr
	return nil
}
