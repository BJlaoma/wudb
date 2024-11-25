package manager

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// 测试前的准备工作
func setupTest(t *testing.T) func() {
	// 创建测试目录
	err := os.MkdirAll(DBFileDir, 0755)
	if err != nil {
		t.Fatalf("设置测试环境失败: %v", err)
	}

	// 返回清理函数
	return func() {
		err := os.RemoveAll(DBFileDir)
		if err != nil {
			t.Errorf("清理测试环境失败: %v", err)
		}
	}
}

// 测试文件创建
func TestFileManager_CreateFile(t *testing.T) {
	cleanup := setupTest(t)
	defer cleanup()

	fm := &FileManager{}

	// 标记测试可以并行执行
	t.Parallel()

	// 定义测试用例
	tests := []struct {
		name     string // 测试用例名称
		filename string // 输入的文件名
		wantErr  bool   // 是否期望错误
	}{
		{
			name:     "正常创建文件",
			filename: "test1",
			wantErr:  false,
		},
		{
			name:     "创建已存在的文件",
			filename: "test1",
			wantErr:  true,
		},
		{
			name:     "创建带后缀的文件",
			filename: "test2.wdb",
			wantErr:  false,
		},
	}

	// 运行测试用例
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := fm.CreateFile(tt.filename)
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateFile() error = %v, wantErr %v", err, tt.wantErr)
			}

			// 验证文件是否真的创建了
			if !tt.wantErr {
				filename := tt.filename
				if !strings.HasSuffix(filename, DBFileSuffix) {
					filename += DBFileSuffix
				}
				fullPath := filepath.Join(DBFileDir, filename)
				if _, err := os.Stat(fullPath); os.IsNotExist(err) {
					t.Errorf("文件未被创建: %s", fullPath)
				}
			}
		})
	}

	// 使用 t.Cleanup 注册清理函数，它会在测试结束后自动执行
	t.Cleanup(func() {
		// 这里的代码会在测试结束后自动执行
		os.RemoveAll(DBFileDir)
	})

	// 也可以使用 defer，但 t.Cleanup 更推荐
	defer func() {
		// 这里的代码也会在测试结束后执行
	}()

	// 使用 Helper 函数包装通用检查逻辑
	t.Helper() // 标记这是一个辅助函数，错误栈会更准确

	// 使用 t.Run 进行子测试
	t.Run("subtest", func(t *testing.T) {
		t.Parallel() // 子测试也可以并行
	})
}

// 测试文件删除
func TestFileManager_DestroyFile(t *testing.T) {
	cleanup := setupTest(t)
	defer cleanup()

	fm := &FileManager{}
	t.Parallel()

	// 创建测试文件
	testFile := "testDestroy"
	if err := fm.CreateFile(testFile); err != nil {
		t.Fatalf("创建测试文件失败: %v", err)
	}

	tests := []struct {
		name     string
		filename string
		wantErr  bool
	}{
		{
			name:     "删除存在的文件",
			filename: "testDestroy",
			wantErr:  false,
		},
		{
			name:     "删除不存在的文件",
			filename: "nonexistent",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := fm.DestroyFile(tt.filename)
			if (err != nil) != tt.wantErr {
				t.Errorf("DestroyFile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// 测试文件打开
func TestFileManager_OpenFile(t *testing.T) {
	cleanup := setupTest(t)
	defer cleanup()

	fm := &FileManager{}

	// 创建测试文件
	testFile := "testOpen"
	if err := fm.CreateFile(testFile); err != nil {
		t.Fatalf("创建测试文件失败: %v", err)
	}

	tests := []struct {
		name     string
		filename string
		wantErr  bool
	}{
		{
			name:     "打开存在的文件",
			filename: "testOpen",
			wantErr:  false,
		},
		{
			name:     "打开不存在的文件",
			filename: "nonexistent",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handle, err := fm.OpenFile(tt.filename)
			if (err != nil) != tt.wantErr {
				t.Errorf("OpenFile() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				defer handle.Close()
			}
		})
	}
}

// 测试文件头部初始化
func TestFileManager_InitFileHeader(t *testing.T) {
	fm := &FileManager{}

	header, err := fm.InitFileHeader()
	if err != nil {
		t.Errorf("InitFileHeader() error = %v, want nil", err)
	}
	if header == nil {
		t.Error("InitFileHeader() returned nil header")
	}

	// 验证魔数是否正确
	if !header.ValidateMagic() {
		t.Error("InitFileHeader() created header with invalid magic number")
	}
}

// 测试文件头部的读取和更新
func TestFileManager_FileHeaderOperations(t *testing.T) {
	cleanup := setupTest(t)
	defer cleanup()

	fm := &FileManager{}
	testFile := "testHeader"

	t.Log("开始测试文件头部操作")

	// 创建测试文件
	if err := fm.CreateFile(testFile); err != nil {
		t.Fatalf("创建测试文件失败: %v", err)
	}
	t.Log("成功创建测试文件")

	// 打开文件
	handle, err := fm.OpenFile(testFile)
	if err != nil {
		t.Fatalf("打开文件失败: %v", err)
	}
	defer func() {
		if err := handle.Close(); err != nil {
			t.Errorf("关闭文件失败: %v", err)
		}
	}()
	t.Log("成功打开文件")

	// 测试获取文件头
	t.Run("GetFileHeader", func(t *testing.T) {
		t.Log("开始测试获取文件头")

		// 打开文件
		handle, err := fm.OpenFile(testFile)
		if err != nil {
			t.Fatalf("打开文件失败: %v", err)
		}
		defer func() {
			if err := handle.Close(); err != nil {
				t.Errorf("关闭文件失败: %v", err)
			}
		}()

		// 确保文件指针在开始位置
		handle.SetOffset(0)

		header, err := fm.GetFileHeader(handle)
		if err != nil {
			t.Errorf("GetFileHeader() error = %v", err)
			return
		}

		t.Logf("获取到的文件头: %+v", header)

		if header == nil {
			t.Error("GetFileHeader() returned nil header")
			return
		}

		if !header.ValidateMagic() {
			t.Errorf("GetFileHeader() returned header with invalid magic number: %x", header.Magic)
		}
	})

	// 测试更新文件头
	t.Run("UpdateFileHeader", func(t *testing.T) {
		t.Log("开始测试更新文件头")

		// 确保文件指针在开始位置
		handle.SetOffset(0)
		// 获取原始文件头
		originalHeader, err := fm.GetFileHeader(handle)
		if err != nil {
			t.Fatalf("获取原始文件头失败: %v", err)
		}
		t.Logf("原始文件头: %+v", originalHeader)

		// 修改文件头的某些值
		originalHeader.PageCount = 1
		t.Log("修改文件头 PageCount = 1")

		// 确保文件指针在开始位置
		handle.SetOffset(0)

		// 更新文件头
		if err := fm.UpdateFileHeader(handle, originalHeader); err != nil {
			t.Errorf("UpdateFileHeader() error = %v", err)
			return
		}
		t.Log("文件头更新完成")
		//输出文件大小
		t.Logf("文件大小: %v", handle.GetFileSize())
		// 确保文件指针在开始位置
		handle.SetOffset(0)
		if !handle.IsFileOpen() {
			t.Log("文件未打开")
		}

		// 重新读取文件头验证更新是否成功
		newHeader, err := fm.GetFileHeader(handle)
		if err != nil {
			t.Fatalf("验证更新后获取文件头失败: %v", err)
		}
		t.Logf("更新后的文件头: %+v", newHeader)

		// 验证更新是否成功
		if newHeader.PageCount != originalHeader.PageCount {
			t.Errorf("UpdateFileHeader() failed: got PageCount = %v, want %v",
				newHeader.PageCount, originalHeader.PageCount)
		}

		// 验证魔数是否保持不变
		if newHeader.Magic != originalHeader.Magic {
			t.Errorf("UpdateFileHeader() corrupted magic number: got %x, want %x",
				newHeader.Magic, originalHeader.Magic)
		}
	})
}

// 测试文件句柄的设置
func TestFileManager_SetFileHandle(t *testing.T) {
	cleanup := setupTest(t)
	defer cleanup()

	fm := &FileManager{}
	testFile := "testSetHandle"

	// 创建测试文件
	if err := fm.CreateFile(testFile); err != nil {
		t.Fatalf("创建测试文件失败: %v", err)
	}

	// 获取文件句柄
	handle, err := fm.OpenFile(testFile)
	if err != nil {
		t.Fatalf("获取文件句柄失败: %v", err)
	}
	defer handle.Close()

	// 测试设置文件句柄
	fm.SetFileHandle(handle)

	// 验证文件句柄是否正确设置
	if fm.fileHandle != handle {
		t.Error("SetFileHandle() failed: fileHandle not properly set")
	}
}
