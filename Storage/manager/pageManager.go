package manager

import (
	"fmt"
	"log"
	"time"
	"wudb/Entity/Page"
	"wudb/Entity/Record"
	"wudb/Util"
)

type PageManager struct {
	fileHandle *Util.FileHandle
	pageID     uint32
	metaPage   *Page.PageBPlusTree
}

func NewPageManager(fileHandle *Util.FileHandle) *PageManager {
	pm := &PageManager{
		fileHandle: fileHandle,
	}

	// 尝试读取已存在的MetaPage
	meta, err := pm.GetMetaPage()
	if err != nil {
		// 如果读取失败，说明MetaPage不存在，需要初始化
		if err := pm.InitMetaPage(); err != nil {
			log.Printf("初始化MetaPage失败: %v", err)
			return nil
		}
		meta, _ = pm.GetMetaPage()
	}

	pm.metaPage = meta
	return pm
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

func (pm *PageManager) CreatePage(pageType uint32) (*Page.Page, error) {
	page := Page.NewPage()
	page.Header.PageType = pageType
	meta, err := pm.GetMetaPage()
	if err != nil {
		return nil, err
	}

	page.Header.PageID = meta.LastPageID + 1
	meta.LastPageID++
	page.Header.CreateTime = uint32(time.Now().Unix())
	page.Header.ModifyTime = page.Header.CreateTime

	// 序列化并写入页面
	data, err := page.SerializeTo()
	if err != nil {
		return nil, fmt.Errorf("序列化页面失败: %v", err)
	}

	offset := int64(FileHeaderSize) + int64(page.Header.PageID)*int64(PageSize)
	pm.fileHandle.SetOffset(offset)

	if _, err := pm.fileHandle.Write(data); err != nil {
		return nil, fmt.Errorf("写入页面失败: %v", err)
	}

	// 更新元数据
	meta.PageCount++
	if err := pm.WriteMetaPage(); err != nil {
		return nil, err
	}

	return page, nil
}

// 分裂叶子节点
func (pm *PageManager) splitLeafPage(page *Page.Page) (*Page.Page, *Page.Page, *Record.InternalRecord) {
	newPage, err := pm.CreatePage(Page.LeafPageID)
	if err != nil {
		return nil, nil, nil
	}
	key, value, recordKey := page.SplitKeyAndValue()
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
		KeySize:       Record.KeySize,
		ValueSize:     Record.ValueSize,
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

/*
// root页面相关

	func (pm *PageManager) CreateRootPage() *Page.PageBPlusTree {
		pm.rootPage = Page.NewPageBPlusTree()
		pm.rootPage.Header.PageType = Page.MetaPageID
		pm.rootPage.Header.PageID = 0
		pm.rootPage.Header.CreateTime = uint32(time.Now().Unix())
		pm.rootPage.Header.ModifyTime = pm.rootPage.Header.CreateTime
		pm.rootPage.SetRootPageID(0)
		pm.rootPage.SetFirstLeafPageID(0)
		pm.rootPage.SetLastLeafPageID(0)
		pm.rootPage.SetTreeHeight(0)
		pm.rootPage.SetPageCount(0)
		return pm.rootPage
	}
*/
func (pm *PageManager) InitRootPage() error {
	root, err := pm.CreateRootPage()
	if err != nil {
		return fmt.Errorf("创建根页面失败: %v", err)
	}
	data, err := root.SerializeTo()
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

func (pm *PageManager) GetROOTPage() (*Page.Page, error) {
	metaPage := Page.NewPageBPlusTree()
	offset := int64(FileHeaderSize)
	pm.fileHandle.SetOffset(offset)
	data, err := pm.fileHandle.Read(PageSize)
	if err != nil {
		return nil, err
	}
	if err := metaPage.DeserializeFrom(data); err != nil {
		return nil, fmt.Errorf("解析页面数据失败: %v", err)
	}
	rootid := metaPage.GetRootPageID()
	page, err := pm.GetPage(rootid)
	if err != nil {
		return nil, fmt.Errorf("获取页面失败: %v", err)
	}
	pm.metaPage = metaPage
	return page, nil
}

func (pm *PageManager) WriteMetaPage() error {
	data, err := pm.metaPage.SerializeTo()
	if err != nil {
		return fmt.Errorf("序列化元数据页失败: %v", err)
	}
	offset := int64(FileHeaderSize)
	pm.fileHandle.SetOffset(offset)
	_, err = pm.fileHandle.Write(data)
	if err != nil {
		return fmt.Errorf("写入元数据页失败: %v", err)
	}
	return nil
}

// 释放页面
func (pm *PageManager) DisposePage(page *Page.Page) error {
	// 这里可以实现页面回收的逻辑
	// 简单实现：将页面标记为已删除
	page.Header.IsDeleted = 1
	return pm.UpdatePage(page)
}

// 初始化元数据页面
func (pm *PageManager) InitMetaPage() error {
	pm.metaPage = Page.NewPageBPlusTree()
	pm.metaPage.Header.PageType = Page.MetaPageID
	pm.metaPage.Header.PageID = 0
	pm.metaPage.Header.CreateTime = uint32(time.Now().Unix())
	pm.metaPage.Header.ModifyTime = pm.metaPage.Header.CreateTime
	pm.metaPage.RootPageID = 0
	pm.metaPage.FirstPageID = 0
	pm.metaPage.LastPageID = 0
	pm.metaPage.TreeHeight = 0
	pm.metaPage.PageCount = 0

	// 写入元数据页
	data, err := pm.metaPage.SerializeTo()
	if err != nil {
		return fmt.Errorf("序列化MetaPage失败: %v", err)
	}

	offset := int64(FileHeaderSize)
	pm.fileHandle.SetOffset(offset)
	_, err = pm.fileHandle.Write(data)
	return err
}

// 获取元数据页面
func (pm *PageManager) GetMetaPage() (*Page.PageBPlusTree, error) {
	if pm.metaPage != nil {
		return pm.metaPage, nil
	}

	metaPage := Page.NewPageBPlusTree()
	offset := int64(FileHeaderSize)
	pm.fileHandle.SetOffset(offset)

	data, err := pm.fileHandle.Read(PageSize)
	if err != nil {
		return nil, err
	}

	if err := metaPage.DeserializeFrom(data); err != nil {
		return nil, fmt.Errorf("解析MetaPage失败: %v", err)
	}

	pm.metaPage = metaPage
	return metaPage, nil
}

// 获取根页面
func (pm *PageManager) GetRootPage() (*Page.Page, error) {
	meta, err := pm.GetMetaPage()
	if err != nil {
		return nil, err
	}

	return pm.GetPage(meta.RootPageID)
}

// 创建新的根页面
func (pm *PageManager) CreateRootPage() (*Page.Page, error) {
	// 创建新页面作为根节点
	rootPage := Page.NewPage()
	rootPage.Header.PageType = Page.LeafPageID

	// 写入新页面
	if err := pm.UpdatePage(rootPage); err != nil {
		return nil, err
	}

	// 更新元数据
	pm.metaPage.RootPageID = rootPage.Header.PageID
	pm.metaPage.FirstPageID = rootPage.Header.PageID
	pm.metaPage.LastPageID = rootPage.Header.PageID
	pm.metaPage.TreeHeight = 1

	// 保存元数据更新
	return rootPage, pm.WriteMetaPage()
}
