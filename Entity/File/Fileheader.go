package File

import (
	"encoding/binary"
	"fmt"
	"time"
	"wudb/Util"
)

// 文件头大小：64B
type FileHeader struct {
	Magic         uint32   // 魔数，用于识别文件类型
	Version       uint32   // 文件版本
	PageSize      uint32   // 页大小
	PageCount     uint32   // 总页数
	FirstFreePage uint32   // 第一个空闲页的ID
	LastPageID    uint32   // 最后一页的ID
	CreateTime    int64    // 创建时间 2024-11-25 10:00:00 //
	UpdateTime    int64    // 最后更新时间
	Checksum      uint32   // 校验和
	FileSize      uint32   // 文件大小
	Reserved      [16]byte // 保留字段
}

const (
	// WuDB的魔数，用于标识文件类型
	WUDB_MAGIC = 0x57554442 // "WUDB" 的ASCII码
	PAGE_SIZE  = 4096       //4kb
)

// 初始化文件头
func NewFileHeader() *FileHeader {
	return &FileHeader{
		Magic:         WUDB_MAGIC,
		PageCount:     0,
		FirstFreePage: 0,
		CreateTime:    time.Now().Unix(),
		UpdateTime:    time.Now().Unix(),
		Checksum:      0,
		FileSize:      0,
		Version:       0,
		PageSize:      PAGE_SIZE,
	}
}

// 验证魔数是否正确
func (fh *FileHeader) ValidateMagic() bool {
	return fh.Magic == WUDB_MAGIC
}

func (fh *FileHeader) WriteToFile(file *Util.FileHandle) error {
	// 1. 先将文件指针设置到文件开始处
	if _, err := file.GetFile().Seek(0, 0); err != nil {
		return fmt.Errorf("设置文件指针失败: %v", err)
	}

	// 2. 写入文件头
	if err := binary.Write(file.GetFile(), binary.LittleEndian, fh); err != nil {
		return fmt.Errorf("写入文件头失败: %v", err)
	}

	// 3. 确保写入磁盘
	return file.GetFile().Sync()
}

func (fh *FileHeader) GetFileSize() uint32 {
	return fh.FileSize
}

func (fh *FileHeader) SetFileSize(size uint32) {
	fh.FileSize = size
}

func (fh *FileHeader) GetPageCount() uint32 {
	return fh.PageCount
}

func (fh *FileHeader) SetPageCount(count uint32) {
	fh.PageCount = count
}

func (fh *FileHeader) GetFirstFreePage() uint32 {
	return fh.FirstFreePage
}

func (fh *FileHeader) SetFirstFreePage(pageID uint32) {
	fh.FirstFreePage = pageID
}

func (fh *FileHeader) GetLastPageID() uint32 {
	return fh.LastPageID
}

func (fh *FileHeader) SetLastPageID(pageID uint32) {
	fh.LastPageID = pageID
}

func (fh *FileHeader) GetCreateTime() int64 {
	return fh.CreateTime
}

func (fh *FileHeader) SetCreateTime(time int64) {
	fh.CreateTime = time
}

func (fh *FileHeader) GetUpdateTime() int64 {
	return fh.UpdateTime
}

func (fh *FileHeader) SetUpdateTime(time int64) {
	fh.UpdateTime = time
}

func (fh *FileHeader) GetChecksum() uint32 {
	return fh.Checksum
}

func (fh *FileHeader) SetChecksum(checksum uint32) {
	fh.Checksum = checksum
}

func (fh *FileHeader) GetMagic() uint32 {
	return fh.Magic
}

func (fh *FileHeader) SetMagic(magic uint32) {
	fh.Magic = magic
}

func (fh *FileHeader) GetVersion() uint32 {
	return fh.Version
}

func (fh *FileHeader) SetVersion(version uint32) {
	fh.Version = version
}

func (fh *FileHeader) GetPageSize() uint32 {
	return fh.PageSize
}

func (fh *FileHeader) SetPageSize(size uint32) {
	fh.PageSize = size
}
