package memtable

import (
	"bytes"
	"encoding/binary"
)

// MemtableInterface definise zajednicki interfejs za sve implementacije Memtable-a
type MemtableInterface interface {
	Add(key, value string) error
	Delete(key string) error
	Get(key string) (string, bool)
	PrintData()
	LoadFromWAL(walPath string) error
}

// Utility funkcije za konverziju tipova
func TimestampToBytes(ts int64) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, ts)
	return buf.Bytes()
}

func BoolToByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

// Potencijalno možemo premjestiti još generalnih funckija u interfejs da nema redundantnog koda
