package probabilistic

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"os"
)

const (
	HLL_MIN_PRECISION = 4
	HLL_MAX_PRECISION = 16
)

func firstKbits(value, k uint64) uint64 {
	return value >> (64 - k)
}

func trailingZeroBits(value uint64) int {
	return bits.TrailingZeros64(value)
}

type HyperLogLog struct {
	m   uint64
	p   uint8
	reg []uint8
}

func CreateHLL(p uint8) HyperLogLog {
	return HyperLogLog{
		m:   uint64(1 << p),
		p:   p,
		reg: make([]uint8, 1<<p),
	}
}

func (hll *HyperLogLog) emptyCount() int {
	sum := 0
	for _, val := range hll.reg {
		if val == 0 {
			sum++
		}
	}
	return sum
}

func (hll *HyperLogLog) Add(value string) {
	hasher := md5.New()
	hasher.Write([]byte(value))
	hashValue := hasher.Sum(nil)

	hashUint64 := binary.BigEndian.Uint64(hashValue[:8])

	idx := firstKbits(hashUint64, uint64(hll.p))

	zeroCount := trailingZeroBits(hashUint64<<hll.p) + 1

	if zeroCount > int(hll.reg[idx]) {
		hll.reg[idx] = uint8(zeroCount)
	}
}

func (hll *HyperLogLog) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.reg {
		sum += math.Pow(math.Pow(2.0, float64(val)), -1)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.m))
	estimation := alpha * math.Pow(float64(hll.m), 2.0) / sum
	emptyRegs := hll.emptyCount()
	if estimation <= 2.5*float64(hll.m) {
		if emptyRegs > 0 {
			estimation = float64(hll.m) * math.Log(float64(hll.m)/float64(emptyRegs))
		}
	} else if estimation > 1/30.0*math.Pow(2.0, 32.0) {
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

func (hll *HyperLogLog) Reset() {
	for i := range hll.reg {
		hll.reg[i] = 0
	}
}

func (hll *HyperLogLog) serialize() {
	filepath := fmt.Sprintf("files%chll.bin", os.PathSeparator)
	file, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	err = binary.Write(file, binary.BigEndian, hll.p)
	if err != nil {
		panic(err)
	}

	for _, val := range hll.reg {
		err = binary.Write(file, binary.BigEndian, val)
		if err != nil {
			panic(err)
		}
	}
}

func deserialize() HyperLogLog {
	filepath := fmt.Sprintf("files%chll.bin", os.PathSeparator)
	file, err := os.Open(filepath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	var p uint8
	err = binary.Read(file, binary.BigEndian, &p)
	if err != nil {
		panic(err)
	}

	m := uint64(1 << p)

	hll := HyperLogLog{
		m:   m,
		p:   p,
		reg: make([]uint8, m),
	}

	for i := range hll.reg {
		err = binary.Read(file, binary.BigEndian, &hll.reg[i])
		if err != nil {
			panic(err)
		}
	}

	return hll
}
