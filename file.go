package main

import (
	"bytes"
	"os"
)

func sendDataToFile(f *os.File, buf *bytes.Buffer) error {
	if _, err := f.Write(buf.Bytes()); err != nil {
		return err
	}
	return nil
}
