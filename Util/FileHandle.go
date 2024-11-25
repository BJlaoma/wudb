package Util

import (
	"os"
	"sync"
)

type FileHandle struct {
	FileID string // 文件ID
	File   *os.File
	Offset int64        // 文件偏移量
	mutex  sync.RWMutex // 读写锁
}

func NewFileHandle(fileID string, file *os.File) *FileHandle {
	return &FileHandle{FileID: fileID, File: file, Offset: 0}
}

func (f *FileHandle) SetOffset(offset int64) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.Offset = offset
}

func (f *FileHandle) GetFileID() string {
	return f.FileID
}

func (f *FileHandle) GetOffset() int64 {
	return f.Offset
}

func (f *FileHandle) GetFile() *os.File {
	return f.File
}

/*
*
读取文件内容，从Offset位置开始读取
@param p 读取到的内容
@return n 实际读取到的字节数
@return err 错误信息
*/
func (f *FileHandle) Read(p []byte) (n int, err error) {
	f.mutex.RLock()
	defer f.mutex.RUnlock()
	n, err = f.File.ReadAt(p, f.Offset)
	if err == nil {
		f.Offset += int64(n)
	}
	return n, err
}

/*
*
写入文件内容，从Offset位置开始写入
@param p 写入的内容
@return n 实际写入的字节数
@return err 错误信息
*/
func (f *FileHandle) Write(p []byte) (n int, err error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	n, err = f.File.WriteAt(p, f.Offset)
	if err == nil {
		f.Offset += int64(n)
	}
	return n, err
}

/*
*
关闭文件
@return err 错误信息
*/
func (f *FileHandle) Close() error {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	return f.File.Close()
}

func (f *FileHandle) IsFileOpen() bool {
	return f.File != nil
}

func (f *FileHandle) GetFileSize() int64 {
	stat, err := f.File.Stat()
	if err != nil {
		return 0
	}
	return stat.Size()
}
