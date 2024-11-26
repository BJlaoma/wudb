package Util

import (
	"fmt"
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
func (f *FileHandle) Read(length int64) ([]byte, error) {
	f.mutex.RLock()
	defer f.mutex.RUnlock()

	// 创建缓冲区
	data := make([]byte, length)

	// 从指定偏移量读取数据
	n, err := f.File.ReadAt(data, f.Offset)
	if err != nil {
		return nil, fmt.Errorf("读取文件失败: %v", err)
	}

	// 更新偏移量
	f.Offset += int64(n)

	// 如果读取的数据长度不足，返回错误
	if int64(n) < length {
		return data[:n], fmt.Errorf("读取数据不完整: 期望 %d 字节, 实际读取 %d 字节", length, n)
	}

	return data, nil
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
