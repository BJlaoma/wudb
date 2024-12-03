package Util

import "os"

type FileMaker struct {
}

func (fm *FileMaker) CreateFile(filename string) (*FileHandle, error) {
	file, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	return NewFileHandle(filename, file), nil
}
