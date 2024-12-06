package containers

type Container interface {
	WriteElement(key string, value []byte) error
	ReadElement(key string) ([]byte, error)
}
