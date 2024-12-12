package config

// Struktura koja se podudara sa JSON strukturom
type Config struct {
	MaxMemtableSize   int    `json:"MaxMemtableSize"`
	Num_memtables     int    `json:"Num_memtables"`
	Memtable_struct   string `json:"Memtable_struct"`
	WALmaxSegmentSize int    `json:"WALmaxSegmentSize"`
	BlockSize         int    `json:"BlockSize"`
	FilePath          string `json:"FilePath"`
}
