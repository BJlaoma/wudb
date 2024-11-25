package manager

import "wudb/Util"

type PageManager struct {
	fileHandle *Util.FileHandle
	pageID     uint32
}

const (
	PageHeaderSize = 41
	PageSize       = 4096
)
