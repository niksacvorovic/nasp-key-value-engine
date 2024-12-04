package config

// Struct to match the JSON structure
type Config struct {
	MaxMemtableSize   int     `json:"MaxMemtableSize"`
	Num_memtables     int     `json:"Num_memtables"`
	Memtable_struct   string  `json:"Memtable_struct"`
	WALmaxSegmentSize int     `json:"WALmaxSegmentSize"`
}