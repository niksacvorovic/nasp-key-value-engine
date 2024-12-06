package memtable

import (
	"projekat/structs/containers"
)

type Memtable struct {
	ContainerInstance containers.Container
}

func Add(key, value string) error
func Delete(key string) error
func Get(key string) (string, bool)
func PrintData()
func LoadFromWAL(walPath string) error
