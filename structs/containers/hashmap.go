package containers

import (
	"errors"
)

type HashMap struct {
	elements map[string][]byte
}

func CreateHM() *HashMap {
	return &HashMap{make(map[string][]byte)}
}

func (hm *HashMap) ReadElement(key string) ([]byte, error) {
	value, ok := hm.elements[key]
	if !ok {
		return []byte{}, errors.New("nonexistent value")
	}
	return value, nil
}

func (hm *HashMap) WriteElement(key string, value []byte) error {
	_, inmap := hm.elements[key]
	if inmap {
		return errors.New("duplicate element")
	}
	hm.elements[key] = value
	return nil
}
