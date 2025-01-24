package config

// Struktura koja se podudara sa JSON strukturom
type Config struct {
	MaxMemtableSize   int    `json:"MaxMemtableSize"`
	Num_memtables     int    `json:"Num_memtables"`
	Memtable_struct   string `json:"Memtable_struct"`
	SkipListLevelNum  int    `json:"SkipListLevelNum"`
	WALmaxSegmentSize int    `json:"WALmaxSegmentSize"`
	BlockSize         int    `json:"BlockSize"`
	BlockCacheSize    int    `json:"BlockCacheSize"`
	FilePath          string `json:"FilePath"`
	TokenRate         int    `json:"TokenRate"`
	TokenInterval     int    `json:"TokenInterval"`
}
