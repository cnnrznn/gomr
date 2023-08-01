package store

import (
	"fmt"
	"os"
	"strings"
)

type FileStore struct {
	pointer  int
	size     int
	file     os.File
	Filename string
}

func (f *FileStore) Init() error {

}

func (f *FileStore) More() bool {
	stat, err := f.file.Stat()
	if err != nil {
		fmt.Println(err)
	}

	f.size = int(stat.Size())

	if f.pointer < f.size {
		return true
	}
	return false
}

func (f *FileStore) Read() ([]byte, error) {
	if !f.More() {
		return nil, fmt.Errorf("no more bytes to read")
	}

	buf := make([]byte, 8192)
	n, err := f.file.Read(buf)
	if err != nil {
		return nil, err
	}

	ls := strings.Split(string(buf[:n]), "\n")
	offset := n - len([]byte(ls[0]))

	f.file.Seek(-1*int64(offset), 1)
	f.pointer += len([]byte(ls[0]))

	return []byte(ls[0]), nil
}

func (f *FileStore) Write(bs []byte) error {
	n, err := f.file.Write(bs)
	if err != nil {
		return err
	}
	if n != len(bs) {
		return fmt.Errorf("couldn't write all bytes to file")
	}

	return nil
}
