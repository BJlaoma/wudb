package manager

import (
	"testing"
	"wudb/Entity/Page"
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

// 测试MetaPage的创建和读取
func TestPageManager_MetaPage(t *testing.T) {
	pm, _, cleanup := setupPageManagerTest(t)
	defer cleanup()

	t.Log("开始测试MetaPage")

	// 获取MetaPage
	meta, err := pm.GetMetaPage()
	if err != nil {
		t.Fatalf("获取MetaPage失败: %v", err)
	}

	// 验证MetaPage的初始状态
	if meta == nil {
		t.Fatal("MetaPage为空")
	}
	if meta.Header.PageType != Page.MetaPageID {
		t.Error("页面类型不正确")
	}
	if meta.Header.PageID != 0 {
		t.Error("页面ID不正确")
	}
	if meta.RootPageID != 0 {
		t.Error("根页面ID不正确")
	}
	if meta.TreeHeight != 0 {
		t.Error("树高度不正确")
	}
	if meta.PageCount != 0 {
		t.Error("页面数量不正确")
	}
}

// 测试创建普通页面
func TestPageManager_CreatePage(t *testing.T) {
	pm, _, cleanup := setupPageManagerTest(t)
	defer cleanup()

	t.Log("开始测试创建普通页面")

	// 创建第一个页面
	page1, err := pm.CreatePage(Page.LeafPageID)
	if err != nil {
		t.Fatalf("创建第一个页面失败: %v", err)
	}

	// 验证页面ID
	if page1.Header.PageID != 1 {
		t.Errorf("页面ID不正确: 期望 1, 实际 %d", page1.Header.PageID)
	}

	// 创建第二个页面
	page2, err := pm.CreatePage(Page.LeafPageID)
	if err != nil {
		t.Fatalf("创建第二个页面失败: %v", err)
	}

	// 验证页面ID递增
	if page2.Header.PageID != 2 {
		t.Errorf("页面ID不正确: 期望 2, 实际 %d", page2.Header.PageID)
	}

	// 验证MetaPage更新
	meta, err := pm.GetMetaPage()
	if err != nil {
		t.Fatalf("获取MetaPage失败: %v", err)
	}
	if meta.PageCount != 2 {
		t.Errorf("页面数量不正确: 期望 2, 实际 %d", meta.PageCount)
	}
}

// 测试读取页面
func TestPageManager_GetPage(t *testing.T) {
	pm, _, cleanup := setupPageManagerTest(t)
	defer cleanup()

	t.Log("开始测试读取页面")

	// 创建一个测试页面
	originalPage, err := pm.CreatePage(Page.LeafPageID)
	if err != nil {
		t.Fatalf("创建页面失败: %v", err)
	}

	// 写入一些测试数据
	testKey := []byte("testkey")
	if err := originalPage.WriteKey(0, testKey); err != nil {
		t.Fatalf("写入Key失败: %v", err)
	}

	// 保存页面
	if err := pm.UpdatePage(originalPage); err != nil {
		t.Fatalf("更新页面失败: %v", err)
	}

	// 重新读取页面
	fetchedPage, err := pm.GetPage(originalPage.Header.PageID)
	if err != nil {
		t.Fatalf("获取页面失败: %v", err)
	}

	// 验证页面内容
	readKey, err := fetchedPage.ReadKey(0, uint32(len(testKey)))
	if err != nil {
		t.Fatalf("读取Key失败: %v", err)
	}
	if string(readKey) != string(testKey) {
		t.Error("页面内容不匹配")
	}
}

// 测试创建根页面
func TestPageManager_CreateRootPage(t *testing.T) {
	pm, _, cleanup := setupPageManagerTest(t)
	defer cleanup()

	t.Log("开始测试创建根页面")

	// 创建根页面
	rootPage, err := pm.CreateRootPage()
	if err != nil {
		t.Fatalf("创建根页面失败: %v", err)
	}

	// 验证根页面属性
	if rootPage.Header.PageType != Page.LeafPageID {
		t.Error("根页面类型不正确")
	}

	// 验证MetaPage更新
	meta, err := pm.GetMetaPage()
	if err != nil {
		t.Fatalf("获取MetaPage失败: %v", err)
	}

	if meta.RootPageID != rootPage.Header.PageID {
		t.Error("MetaPage中的根页面ID不正确")
	}
	if meta.TreeHeight != 1 {
		t.Error("树高度不正确")
	}
}

// 测试页面更新
func TestPageManager_UpdatePage(t *testing.T) {
	pm, _, cleanup := setupPageManagerTest(t)
	defer cleanup()

	t.Log("开始测试页面更新")

	// 创建测试页面
	page, err := pm.CreatePage(Page.LeafPageID)
	if err != nil {
		t.Fatalf("创建页面失败: %v", err)
	}

	// 修改页面内容
	testData := []byte("test data")
	if err := page.WriteKey(0, testData); err != nil {
		t.Fatalf("写入数据失败: %v", err)
	}

	// 更新页面
	if err := pm.UpdatePage(page); err != nil {
		t.Fatalf("更新页面失败: %v", err)
	}

	// 重新读取页面验证更新
	updatedPage, err := pm.GetPage(page.Header.PageID)
	if err != nil {
		t.Fatalf("获取更新后的页面失败: %v", err)
	}

	// 验证更新内容
	readData, err := updatedPage.ReadKey(0, uint32(len(testData)))
	if err != nil {
		t.Fatalf("读取数据失败: %v", err)
	}
	if string(readData) != string(testData) {
		t.Error("页面更新内容不匹配")
	}
}
