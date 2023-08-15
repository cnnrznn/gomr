package store

type Store interface {
	Init() error
	Close() error

	More() bool
	Read() ([]byte, error)
	Write(bs []byte) error
}
