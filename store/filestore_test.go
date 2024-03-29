package store

import (
	"fmt"
	"os"
	"testing"
)

func TestFileStore(t *testing.T) {
	var fs Store
	var fn string = "test.txt"

	fs = &FileStore{
		Filename: "test.txt",
	}

	f, err := os.OpenFile(fn, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		t.Error(err)
	}
	f.Close()

	err = fs.Init()
	if err != nil {
		t.Error(err.Error())
	}

	fs.Write([]byte("This is a line"))
	fs.Write([]byte("another line"))
	fs.Write([]byte("This is a line"))
	fs.Init()

	lines := 0
	for fs.More() {
		fs.Read()
		lines++
	}

	if lines != 3 {
		t.Error(fmt.Errorf("Unexpected number of lines in file"))
	}
}
