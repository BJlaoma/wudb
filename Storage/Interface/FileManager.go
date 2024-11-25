package Storage

import (
	"wudb/Util"
)

type fileManager interface {
	CreateFile(filename string) error
	DestroyFile(filename string) error
	OpenFile(filename string) (*Util.FileHandle, error)
	CloseFile(file *Util.FileHandle) error
}
