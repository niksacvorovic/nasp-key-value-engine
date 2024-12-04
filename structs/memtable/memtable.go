package memtable

// MemtableInterface definise zajednicki interfejs za sve implementacije Memtable-a
type MemtableInterface interface {
	Add(key, value string) error
	Delete(key string) error
	Get(key string) (string, bool)
	PrintData()
	LoadFromWAL(walPath string) error
}