package config

// Struktura koja se podudara sa JSON strukturom
type Config struct {
	// Memtable
	MaxMemtableSize  int    `json:"MaxMemtableSize"`
	MemtableNum      int    `json:"MemtableNum"`
	MemtableStruct   string `json:"MemtableStruct"`
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
	WalBlocksPerSegment     int `json:"WalBlocksPerSegment"`

	// SSTable
	SummaryStep       int  `json:"SummaryStep"`
	SSTableSingleFile bool `json:"SSTableSingleFile"`

	// Compactions
	CompactionAlgorithm string `json:"CompactionAlgorithm"`
	MaxCountInLevel     int    `json:"MaxCountInLevel"`
}
