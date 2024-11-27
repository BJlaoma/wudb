package manager

import (
	"wudb/Entity/Page"
	"wudb/Entity/Record"
	"wudb/Util"
)

type RecordManager struct {
	records     map[string]*Record.Record
	fileHandle  *Util.FileHandle
	pageManager *PageManager
}

func (rm *RecordManager) insertRecord(record *Record.Record) *Page.PageHeader {
	key := record.GetKey()
	metaPage := rm.pageManager.rootPage                             // 获取信息页
	currentPage, err := rm.pageManager.GetPage(metaPage.RootPageID) // 获取当前页，一开始是根页
	if err != nil {
		return nil
	}
	for currentPage.Header.PageType == Page.InternalPageID { //内部页面
		internalRecord := currentPage.FindInternalRecord(key)
		currentPage, err = rm.pageManager.GetPage(internalRecord.GetFrontPointer())
		if err != nil {
			return nil
		}
	}
	if currentPage.Header.PageType == Page.LeafPageID { //叶子节点
		//插入记录到叶子节点
		err = currentPage.InsertRecord(record)
		if err != nil {
			if err.Error() == "叶子节点已满" {
				//如果叶子节点满了，则需要分裂叶子节点，分成现有节点和新节点
				_, _, internalRecord := rm.pageManager.splitLeafPage(currentPage)
				rm.InsertInternalRecord(internalRecord)
				return nil
			}
		}

	}
	return nil
}

func (rm *RecordManager) InsertInternalRecord(internalRecord *Record.InternalRecord) {
	key := internalRecord.GetKey()
	metaPage := rm.pageManager.rootPage                             // 获取信息页
	currentPage, err := rm.pageManager.GetPage(metaPage.RootPageID) // 获取当前页，一开始是根页
	if err != nil {
		return
	}
	for currentPage.Header.PageType == Page.InternalPageID { //内部页面
		internalRecord := currentPage.FindInternalRecord(key)
		currentPage, err = rm.pageManager.GetPage(internalRecord.GetFrontPointer())
		if err != nil {
			return
		}
	}
}
