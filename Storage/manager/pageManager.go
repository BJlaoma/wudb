package manager

import (
	"fmt"
	"time"
	"wudb/Entity/Page"
	"wudb/Entity/Record"
	"wudb/Util"
)

type PageManager struct {
	fileHandle *Util.FileHandle
	pageID     uint32
	rootPage   *Page.PageBPlusTree
}

func NewPageManager(fileHandle *Util.FileHandle) *PageManager {
	return &PageManager{
		fileHandle: fileHandle,
	}
}

// 普通页面相关
func (pm *PageManager) GetPage(pageID uint32) (*Page.Page, error) {
	// 创建一个新的页面对象
	page := Page.NewPage()

	// 计算页面在文件中的偏移量
	offset := int64(64) + int64(pageID)*int64(PageSize) // 64byte 是文件头大小

	// 设置文件指针到正确的位置
	pm.fileHandle.SetOffset(offset)

	// 读取页面数据
	data, err := pm.fileHandle.Read(int64(PageSize))
	if err != nil {
		return nil, fmt.Errorf("读取页面失败: %v", err)
	}

	// 解析页面数据
	if err := page.DeserializeFrom(data); err != nil {
		return nil, fmt.Errorf("解析页面数据失败: %v", err)
	}

	return page, nil
}

func (pm *PageManager) CreatePage() (*Page.Page, error) {
	page := Page.NewPage()
	if pm.rootPage == nil {
		pm.GetROOTPage()
	}
	page.Header.PageID = pm.rootPage.LastPageID + 1 // 页号是最后一页+1
	pm.rootPage.LastPageID++
	page.Header.CreateTime = uint32(time.Now().Unix())
	page.Header.ModifyTime = page.Header.CreateTime
	//开始写入
	// 序列化页面
	data, err := page.SerializeTo()
	if err != nil {
		return nil, fmt.Errorf("序列化页面失败: %v", err)
	}

	// 验证数据大小
	if len(data) != PageSize {
		return nil, fmt.Errorf("页面大小错误: 期望 %d 字节, 实际 %d 字节", PageSize, len(data))
	}

	// 计算偏移量
	offset := int64(FileHeaderSize) + int64(page.Header.PageID)*int64(PageSize)
	// 设置文件指针
	pm.fileHandle.SetOffset(offset)

	// 写入数据
	n, err := pm.fileHandle.Write(data)
	if err != nil {
		return nil, fmt.Errorf("写入页面失败: %v", err)
	}
	if n != PageSize {
		return nil, fmt.Errorf("写入不完整: 期望 %d 字节, 实际写入 %d 字节", PageSize, n)
	}
	//更新root页面的lastpageid
	pm.rootPage.LastPageID = page.Header.PageID
	pm.rootPage.PageCount++
	pm.WriteRootPage()
	return page, nil
}

// 分裂叶子节点
func (pm *PageManager) splitLeafPage(page *Page.Page) (*Page.Page, *Page.Page, *Record.InternalRecord) {
	newPage := pm.CreatePage()
	key, value, recordKey := page.splitKeyAndValue()
	newPage.WriteKey(0, key)
	newPage.WriteValue(0, value)
	page.Header.NextPageID = newPage.Header.PageID
	newPage.Header.PrevPageID = page.Header.PageID
	pm.UpdatePage(page)
	pm.UpdatePage(newPage)

	internalRecord := Record.NewInternalRecord(Record.RecordHeader{
		IsDeleted:     0,
		RecordLength:  page.Header.RecordSize,
		TransactionID: 0,
		Timestamp:     uint32(time.Now().Unix()),
		KeySize:       32,
		ValueSize:     page.Header.RecordSize - 32,
		FrontPointer:  page.Header.PageID,
		NextPointer:   newPage.Header.PageID,
	}, recordKey, page.Header.PageID, newPage.Header.PageID)
	return page, newPage, internalRecord
}

/*
func (pm *PageManager) WritePage(page *Page.Page,pageID uint32) error {
	// 1. 先将文件指针设置到该修改的地方
	if _, err := pm.fileHandle.GetFile().Seek(0, 0); err != nil {
		return fmt.Errorf("设置文件指针失败: %v", err)
	}
	if err := binary.Read(pm.fileHandle.GetFile(), binary.LittleEndian, page.Header); err != nil {
		return fmt.Errorf("读取文件头失败: %v", err)
	}
}*/

// 更新页面
func (pm *PageManager) UpdatePage(page *Page.Page) error {
	// 序列化页面
	data, err := page.SerializeTo()
	if err != nil {
		return fmt.Errorf("序列化页面失败: %v", err)
	}

	// 验证数据大小
	if len(data) != PageSize {
		return fmt.Errorf("页面大小错误: 期望 %d 字节, 实际 %d 字节", PageSize, len(data))
	}

	// 计算偏移量
	offset := int64(FileHeaderSize) + int64(page.Header.PageID)*int64(PageSize)

	// 设置文件指针
	pm.fileHandle.SetOffset(offset)

	// 写入数据
	n, err := pm.fileHandle.Write(data)
	if err != nil {
		return fmt.Errorf("写入页面失败: %v", err)
	}
	if n != PageSize {
		return fmt.Errorf("写入不完整: 期望 %d 字节, 实际写入 %d 字节", PageSize, n)
	}

	return nil
}

// root页面相关
func (pm *PageManager) CreateRootPage() *Page.PageBPlusTree {
	pm.rootPage = Page.NewPageBPlusTree()
	pm.rootPage.Header.PageType = Page.MetaPageID
	pm.rootPage.Header.PageID = 0
	pm.rootPage.Header.CreateTime = uint32(time.Now().Unix())
	pm.rootPage.Header.ModifyTime = pm.rootPage.Header.CreateTime
	pm.rootPage.LastPageID = 0
	return pm.rootPage
}

func (pm *PageManager) InitRootPage() error {
	data, err := pm.CreateRootPage().SerializeTo()

	if err != nil {
		return fmt.Errorf("序列化页面失败: %v", err)
	}
	offset := int64(FileHeaderSize)
	pm.fileHandle.SetOffset(offset)
	_, err = pm.fileHandle.Write(data)
	if err != nil {
		return fmt.Errorf("写入页面失败: %v", err)
	}
	return nil
}

func (pm *PageManager) GetROOTPage() (*Page.PageBPlusTree, error) {
	if pm.rootPage != nil {
		return pm.rootPage, nil
	}
	page := Page.NewPageBPlusTree()
	offset := int64(FileHeaderSize)
	pm.fileHandle.SetOffset(offset)
	data, err := pm.fileHandle.Read(PageSize)
	if err != nil {
		return nil, err
	}
	if err := page.DeserializeFrom(data); err != nil {
		return nil, fmt.Errorf("解析页面数据失败: %v", err)
	}
	pm.rootPage = page
	return page, nil
}

func (pm *PageManager) WriteRootPage() error {
	data, err := pm.rootPage.SerializeTo()
	if err != nil {
		return fmt.Errorf("序列化页面失败: %v", err)
	}
	offset := int64(FileHeaderSize)
	pm.fileHandle.SetOffset(offset)
	_, err = pm.fileHandle.Write(data)
	if err != nil {
		return fmt.Errorf("写入页面失败: %v", err)
	}
	return nil
}
