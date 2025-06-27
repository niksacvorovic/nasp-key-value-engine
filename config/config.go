package config

// Struktura koja se podudara sa JSON strukturom
type Config struct {
	// Memtable
	MaxMemtableSize  int    `json:"MaxMemtableSize"`
	Num_memtables    int    `json:"Num_memtables"`
	Memtable_struct  string `json:"Memtable_struct"`
	SkipListLevelNum int    `json:"SkipListLevelNum"`

	// Block Manager and Block Cache
	BlockSize      int `json:"BlockSize"`
	BlockCacheSize int `json:"BlockCacheSize"`
	LRUCacheSize   int `json:"LRUCacheSize"`

	// Access Control
	TokenRate     int `json:"TokenRate"`
	TokenInterval int `json:"TokenInterval"`

	// WAL
	WalMaxRecordsPerSegment int `json:"WalMaxRecordsPerSegment"`
	WalBlokcsPerSegment     int `json:"WalBlokcsPerSegment"`

	// SSTable
	SummaryStep       int  `json:"SummaryStep"`
	SSTableSingleFile bool `json:"SSTableSingleFile"`

	// Compactions
	CompactionAlgorithm string `json:"CompactionAlgorithm"`
	MaxCountInLevel     int    `json:"MaxCountInLevel"`
}
