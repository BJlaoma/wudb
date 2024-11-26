package manager

import (
	"testing"
)

// 测试环境设置
func setupPageManagerTest(t *testing.T) (*PageManager, *FileManager, func()) {
	// 创建文件管理器
	fm := &FileManager{}
	testFile := "test_page_manager"

	// 创建测试文件
	if err := fm.CreateFile(testFile); err != nil {
		t.Fatalf("创建测试文件失败: %v", err)
	}

	// 打开文件
	handle, err := fm.OpenFile(testFile)
	if err != nil {
		t.Fatalf("打开文件失败: %v", err)
	}

	// 创建页面管理器
	pm := NewPageManager(handle)

	// 返回清理函数
	cleanup := func() {
		handle.Close()
		fm.DestroyFile(testFile)
	}

	return pm, fm, cleanup
}

// 测试初始化根页面
func TestPageManager_InitRootPage(t *testing.T) {
	pm, _, cleanup := setupPageManagerTest(t)
	defer cleanup()

	t.Log("开始测试初始化根页面")
	fileSize := pm.fileHandle.GetFileSize()

	// 测试初始化根页面
	err := pm.InitRootPage()
	if err != nil {
		t.Errorf("初始化根页面失败: %v", err)
	}

	// 验证文件大小
	fileSize = pm.fileHandle.GetFileSize()
	expectedSize := int64(FileHeaderSize + PageSize)
	if fileSize != expectedSize {
		t.Errorf("文件大小不正确: 期望 %d, 实际 %d", expectedSize, fileSize)
	}
}

// 测试获取根页面
func TestPageManager_GetROOTPage(t *testing.T) {
	pm, _, cleanup := setupPageManagerTest(t)
	defer cleanup()

	t.Log("开始测试获取根页面")

	// 先初始化根页面
	err := pm.InitRootPage()
	if err != nil {
		t.Fatalf("初始化根页面失败: %v", err)
	}

	// 测试获取根页面
	rootPage, err := pm.GetROOTPage()
	if err != nil {
		t.Errorf("获取根页面失败: %v", err)
	}

	// 验证根页面
	if rootPage == nil {
		t.Error("获取到的根页面为空")
	}

	// 验证页面大小
	data, err := rootPage.SerializeTo()
	if err != nil {
		t.Errorf("序列化页面失败: %v", err)
	}
	if len(data) != PageSize {
		t.Errorf("页面大小不正确: 期望 %d, 实际 %d", PageSize, len(data))
	}
}

// 测试根页面的读写一致性
func TestPageManager_RootPageConsistency(t *testing.T) {
	pm, _, cleanup := setupPageManagerTest(t)
	defer cleanup()

	t.Log("开始测试根页面的读写一致性")

	// 初始化根页面
	err := pm.InitRootPage()
	if err != nil {
		t.Fatalf("初始化根页面失败: %v", err)
	}

	// 获取根页面并修改
	rootPage, err := pm.GetROOTPage()
	if err != nil {
		t.Fatalf("获取根页面失败: %v", err)
	}

	// 修改页面内容
	rootPage.SetRootPageID(11)

	// 写回页面
	err = pm.WriteRootPage()
	if err != nil {
		t.Fatalf("写回页面失败: %v", err)
	}

	// 重新读取页面
	newRootPage, err := pm.GetROOTPage()
	if err != nil {
		t.Fatalf("重新读取根页面失败: %v", err)
	}

	// 读取并验证数据
	readData := newRootPage.GetRootPageID()
	if err != nil {
		t.Fatalf("读取测试数据失败: %v", err)
	}

	// 验证数据一致性
	if readData != 11 {
		t.Errorf("数据不一致: 期望 %d, 实际 %d", 11, readData)
	}
}

// 测试创建普通页面
func TestPageManager_CreatePage(t *testing.T) {
	pm, _, cleanup := setupPageManagerTest(t)
	defer cleanup()

	t.Log("开始测试创建普通页面")

	// 先初始化根页面
	err := pm.InitRootPage()
	if err != nil {
		t.Fatalf("初始化根页面失败: %v", err)
	}

	// 创建第一个普通页面
	page1, err := pm.CreatePage()
	if err != nil {
		t.Fatalf("创建第一个页面失败: %v", err)
	}

	// 验证页面ID是否正确
	if page1.Header.PageID != 1 {
		t.Errorf("页面ID不正确: 期望 1, 实际 %d", page1.Header.PageID)
	}

	// 创建第二个普通页面
	page2, err := pm.CreatePage()
	if err != nil {
		t.Fatalf("创建第二个页面失败: %v", err)
	}

	// 验证页面ID是否正确递增
	if page2.Header.PageID != 2 {
		t.Errorf("页面ID不正确: 期望 2, 实际 %d", page2.Header.PageID)
	}

	// 验证文件大小
	fileSize := pm.fileHandle.GetFileSize()
	expectedSize := int64(FileHeaderSize + PageSize*3) // 文件头 + root页面 + 2个普通页面
	if fileSize != expectedSize {
		t.Errorf("文件大小不正确: 期望 %d, 实际 %d", expectedSize, fileSize)
	}
}

// 测试获取普通页面
func TestPageManager_GetPage(t *testing.T) {
	pm, _, cleanup := setupPageManagerTest(t)
	defer cleanup()

	t.Log("开始测试获取普通页面")

	// 先初始化根页面
	err := pm.InitRootPage()
	if err != nil {
		t.Fatalf("初始化根页面失败: %v", err)
	}

	// 创建一个页面
	originalPage, err := pm.CreatePage()
	if err != nil {
		t.Fatalf("创建页面失败: %v", err)
	}

	// 获取创建的页面
	fetchedPage, err := pm.GetPage(originalPage.Header.PageID)
	if err != nil {
		t.Fatalf("获取页面失败: %v", err)
	}

	// 验证页面ID
	if fetchedPage.Header.PageID != originalPage.Header.PageID {
		t.Errorf("页面ID不匹配: 期望 %d, 实际 %d",
			originalPage.Header.PageID, fetchedPage.Header.PageID)
	}
}

// 测试更新普通页面
func TestPageManager_UpdatePage(t *testing.T) {
	pm, _, cleanup := setupPageManagerTest(t)
	defer cleanup()

	t.Log("开始测试更新普通页面")

	// 先初始化根页面
	err := pm.InitRootPage()
	if err != nil {
		t.Fatalf("初始化根页面失败: %v", err)
	}

	// 创建一个页面
	page, err := pm.CreatePage()
	if err != nil {
		t.Fatalf("创建页面失败: %v", err)
	}

	// 修改页面内容
	testData := []byte("test data")
	copy(page.Value[:len(testData)], testData)

	// 更新页面
	err = pm.UpdatePage(page)
	if err != nil {
		t.Fatalf("更新页面失败: %v", err)
	}

	// 重新获取页面
	updatedPage, err := pm.GetPage(page.Header.PageID)
	if err != nil {
		t.Fatalf("获取更新后的页面失败: %v", err)
	}

	// 读取并验证数据
	readData := updatedPage.Value[:len(testData)]

	// 验证数据一致性
	if string(readData) != string(testData) {
		t.Errorf("数据不一致: 期望 %s, 实际 %s", string(testData), string(readData))
	}
}

// 测试页面序号的连续性
func TestPageManager_PageIDContinuity(t *testing.T) {
	pm, _, cleanup := setupPageManagerTest(t)
	defer cleanup()

	t.Log("开始测试页面序号的连续性")

	// 初始化根页面
	err := pm.InitRootPage()
	if err != nil {
		t.Fatalf("初始化根页面失败: %v", err)
	}

	// 创建多个页面并验证序号
	expectedPageCount := 5
	var lastPageID uint32 = 0

	for i := 0; i < expectedPageCount; i++ {
		page, err := pm.CreatePage()
		if err != nil {
			t.Fatalf("创建第 %d 个页面失败: %v", i+1, err)
		}

		// 验证页面ID是否连续
		if page.Header.PageID != lastPageID+1 {
			t.Errorf("页面ID不连续: 期望 %d, 实际 %d", lastPageID+1, page.Header.PageID)
		}

		lastPageID = page.Header.PageID
	}

	// 验证root页面中记录的最后页面ID
	rootPage, err := pm.GetROOTPage()
	if err != nil {
		t.Fatalf("获取根页面失败: %v", err)
	}

	if rootPage.LastPageID != uint32(expectedPageCount) {
		t.Errorf("root页面记录的最后页面ID不正确: 期望 %d, 实际 %d",
			expectedPageCount, rootPage.LastPageID)
	}
}
