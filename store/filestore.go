package store

import (
	"fmt"
	"os"
)

type FileStore struct {
	pointer int
	size    int
	file    os.File
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

func (f *FileStore) Read(bs []byte) error {
	if !f.More() {
		return fmt.Errorf("no more bytes to read")
	}

	n, err := f.file.Read(bs)
	if err != nil {
		return err
	}
	if n != len(bs) {
		return fmt.Errorf("Did not read entire buffer")
	}

	return nil
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
