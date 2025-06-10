package probabilistic

import (
	"encoding/binary"
	"math"
	"os"
	"time"
)

type CountMinSketch struct {
	HashFunctions []HashWithSeed
	Matrix        [][]uint32
}

func CalculateM(epsilon float64) uint {
	return uint(math.Ceil(math.E / epsilon))
}

func CalculateK(delta float64) uint {
	return uint(math.Ceil(math.Log(math.E / delta)))
}

func CMS_CreateHashFunctions(k uint32) []HashWithSeed {
	h := make([]HashWithSeed, k)
	ts := uint32(time.Now().Unix())
	for i := uint32(0); i < k; i++ {
		seed := make([]byte, 32)
		binary.BigEndian.PutUint32(seed, ts+i)
		hfn := HashWithSeed{Seed: seed}
		h[i] = hfn
	}
	return h
}

func CreateCountMinSketch(epsilon float64, delta float64) CountMinSketch {
	m := CalculateM(epsilon)
	k := CalculateK(delta)
	matrix := make([][]uint32, k)
	for i, _ := range matrix {
		matrix[i] = make([]uint32, m)
	}
	return CountMinSketch{
		HashFunctions: CMS_CreateHashFunctions(uint32(k)),
		Matrix:        matrix,
	}
}

func (cms *CountMinSketch) Add(elem string) {
	bytes := []byte(elem)
	for i := 0; i < int(len(cms.Matrix)); i++ {
		index := cms.HashFunctions[i].Hash(bytes)
		compressed := index % uint64(len(cms.Matrix[i]))
		cms.Matrix[i][compressed]++
	}
}

func (cms *CountMinSketch) FindCount(elem string) uint32 {
	bytes := []byte(elem)
	count := uint32(math.MaxUint32)
	for i := 0; i < int(len(cms.Matrix)); i++ {
		// da li ovo sme da se castuje u uint32?????????????
		index := cms.HashFunctions[i].Hash(bytes)
		compressed := index % uint64(len(cms.Matrix[i]))
		if count > cms.Matrix[i][compressed] {
			count = cms.Matrix[i][compressed]
		}
	}
	return count
}

func (cms *CountMinSketch) Serialize() []byte {
	bytes := make([]byte, 0)
	k := uint32(len(cms.Matrix))
	bytes = binary.BigEndian.AppendUint32(bytes, k)

	m := uint32(len(cms.Matrix[0]))
	bytes = binary.BigEndian.AppendUint32(bytes, m)
	for i := 0; i < len(cms.Matrix); i++ {
		for j := 0; j < len(cms.Matrix[0]); j++ {
			bytes = binary.BigEndian.AppendUint32(bytes, cms.Matrix[i][j])
		}
	}
	n := uint32(len(cms.HashFunctions))
	bytes = binary.BigEndian.AppendUint32(bytes, n)
	for i := 0; i < int(n); i++ {
		bytes = append(bytes, cms.HashFunctions[i].Seed...)
	}
	return bytes
}

func (cms *CountMinSketch) Deserialize(file *os.File) error {
	// procitaj k - broj hash funkcija
	kbin := make([]byte, 4)
	_, err := file.Read(kbin)
	if err != nil {
		return err
	}
	k := binary.BigEndian.Uint32(kbin)

	// procitaj m - broj kolona u matrici
	mbin := make([]byte, 4)
	_, err = file.Read(mbin)
	if err != nil {
		return err
	}
	m := binary.BigEndian.Uint32(mbin)

	// kreiraj praznu matricu
	matrix := make([][]uint32, k)
	for i := 0; i < int(k); i++ {
		matrix[i] = make([]uint32, m)
	}

	// procitaj sve vrednosti u matrici
	for i := 0; i < int(k); i++ {
		for j := 0; j < int(m); j++ {
			cmsbin := make([]byte, 4)
			_, err = file.Read(cmsbin)
			if err != nil {
				return err
			}
			matrix[i][j] = binary.BigEndian.Uint32(cmsbin)
		}
	}

	// procitaj n - broj seedova u hash funkcijama
	nbin := make([]byte, 4)
	_, err = file.Read(nbin)
	if err != nil {
		return err
	}
	n := binary.BigEndian.Uint32(nbin)

	// procitaj seedove i kreiraj hash funkcije
	hashes := make([]HashWithSeed, n)
	for i := 0; i < int(n); i++ {
		seed := make([]byte, 32)
		_, err = file.Read(seed)
		if err != nil {
			return err
		}
		hashes[i] = HashWithSeed{Seed: seed}
	}

	cms.HashFunctions = hashes
	cms.Matrix = matrix

	return nil
}
