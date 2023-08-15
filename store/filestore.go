package store

import (
	"fmt"
	"os"
	"strings"
)

type FileStore struct {
	pointer  int64
	size     int64
	file     *os.File
	Filename string
}

func (f *FileStore) Init() error {
	file, err := os.OpenFile(f.Filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}

	stat, err := file.Stat()
	if err != nil {
		return err
	}

	f.file = file
	f.pointer = 0
	f.size = stat.Size()

	return nil
}

func (f *FileStore) Close() error {
	return f.file.Close()
}

func (f *FileStore) More() bool {
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
	offset := n - (len([]byte(ls[0])) + 1)

	f.file.Seek(-1*int64(offset), 1)
	f.pointer += int64(len([]byte(ls[0])) + 1)

	return []byte(ls[0]), nil
}

func (f *FileStore) Write(bs []byte) error {
	n, err := f.file.Write(append(bs, '\n'))
	if err != nil {
		return err
	}
	if n != len(bs)+1 {
		return fmt.Errorf("couldn't write all bytes to file")
	}

	return nil
}
