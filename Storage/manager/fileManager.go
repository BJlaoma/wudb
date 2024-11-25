package manager

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
	"wudb/Entity/File"
	"wudb/Util"
)

const (
	FileHeaderSize = 48
	PageHeaderSize = 41
	PageSize       = 4096
	DBFileSuffix   = ".wdb"
	DBFileDir      = "wudb/db/" // 扩展，可以配置
)

type FileManager struct {
	pageManager PageManager
	fileName    string
	fileHandle  *Util.FileHandle
}

func (fm *FileManager) CreateFile(filename string) error {
	// 确保文件名有正确的后缀
	if !strings.HasSuffix(filename, DBFileSuffix) {
		filename = filename + DBFileSuffix
	}

	// 确保目录存在
	if err := os.MkdirAll(DBFileDir, 0755); err != nil {
		return fmt.Errorf("创建目录失败: %v", err)
	}

	// 构建完整的文件路径
	fullPath := filepath.Join(DBFileDir, filename)

	// 检查文件是否已存在
	if _, err := os.Stat(fullPath); err == nil {
		return fmt.Errorf("文件已存在: %s", fullPath)
	}

	// 创建新文件
	file, err := os.Create(fullPath)
	if err != nil {
		return fmt.Errorf("创建文件失败: %v", err)
	}

	// 创建并写入文件头
	header, err := fm.InitFileHeader()
	if err != nil {
		return fmt.Errorf("初始化文件头失败: %v", err)
	}
	fh := Util.NewFileHandle(filename, file)
	fm.SetFileHandle(fh)
	// 将文件头写入文件
	if err := header.WriteToFile(fh); err != nil {
		return fmt.Errorf("写入文件头失败: %v", err)
	}
	defer fh.Close()
	return nil
}

func (fm *FileManager) DestroyFile(filename string) error {
	// 确保文件名有正确的后缀
	if !strings.HasSuffix(filename, DBFileSuffix) {
		filename = filename + DBFileSuffix
	}

	// 用于递归搜索文件的内部函数
	var findAndDestroy func(dir string) error
	findAndDestroy = func(dir string) error {
		// 读取目录内容
		entries, err := os.ReadDir(dir)
		if err != nil {
			return fmt.Errorf("读取目录失败: %v", err)
		}

		// 遍历目录中的所有条目
		for _, entry := range entries {
			fullPath := filepath.Join(dir, entry.Name())

			if entry.IsDir() {
				// 如果是目录，递归搜索
				if err := findAndDestroy(fullPath); err != nil {
					return err
				}
			} else if entry.Name() == filename {
				// 找到目标文件，删除它
				if err := os.Remove(fullPath); err != nil {
					return fmt.Errorf("删除文件失败: %v", err)
				}
				return nil // 找到并删除文件后返回
			}
		}
		return fmt.Errorf("文件不存在: %s", filename)
	}

	// 从DBFileDir开始搜索
	err := findAndDestroy(DBFileDir)
	if err != nil {
		return err
	} else {
		return nil
	}
}

func (fm *FileManager) OpenFile(filename string) (*Util.FileHandle, error) {
	if !strings.HasSuffix(filename, DBFileSuffix) {
		filename = filename + DBFileSuffix
	}
	handle, err := fm.GetFileHandle(filename)
	if err != nil {
		return nil, err
	}
	fm.SetFileHandle(handle)
	return handle, nil
}

func (fm *FileManager) CloseFile(file *Util.FileHandle) error {
	return file.Close()
}

func (fm *FileManager) GetFileHandle(filename string) (*Util.FileHandle, error) {
	file, err := fm.findfile(DBFileDir, filename)
	if err != nil {
		return nil, err
	}
	fileHandle := Util.NewFileHandle(filename, file)
	return fileHandle, nil
}

func (fm *FileManager) findfile(dir string, filename string) (*os.File, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("读取目录失败: %v", err)
	}

	// 遍历目录中的所有条目
	for _, entry := range entries {
		fullPath := filepath.Join(dir, entry.Name())

		if entry.IsDir() {
			// 如果是目录，递归搜索
			if file, err := fm.findfile(fullPath, filename); err != nil {
				return nil, err
			} else {
				return file, nil
			}
		} else if entry.Name() == filename {
			// 找到目标文件,返回文件指针
			// 使用读写模式打开文件
			file, err := os.OpenFile(fullPath, os.O_RDWR, 0666)
			if err != nil {
				return nil, fmt.Errorf("打开文件失败: %v", err)
			}
			return file, nil
		}
	}
	return nil, fmt.Errorf("文件不存在: %s", filename)
}

func (fm *FileManager) SetFileHandle(file *Util.FileHandle) {
	fm.fileHandle = file
}

func (fm *FileManager) InitFileHeader() (*File.FileHeader, error) {
	header := File.NewFileHeader()
	return header, nil
}

func (fm *FileManager) GetFileHeader(file *Util.FileHandle) (*File.FileHeader, error) {
	header := &File.FileHeader{}

	// 1. 先将文件指针设置到文件开始处
	if _, err := file.GetFile().Seek(0, 0); err != nil {
		return nil, fmt.Errorf("设置文件指针失败: %v", err)
	}
	if err := binary.Read(file.GetFile(), binary.LittleEndian, header); err != nil {
		return nil, fmt.Errorf("读取文件头失败: %v", err)
	}
	return header, nil
}

func (fm *FileManager) UpdateFileHeader(file *Util.FileHandle, header *File.FileHeader) error {
	file.SetOffset(0)
	header.UpdateTime = time.Now().Unix()
	return header.WriteToFile(file)
}
