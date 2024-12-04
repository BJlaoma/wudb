package manager

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"wudb/Entity/Page"
	"wudb/Entity/Record"
	"wudb/Transaction"
	"wudb/Util"
)

const (
	ErrPageFull  = Error("页面已满")
	ErrPageSplit = Error("页面需要分裂")
	ErrNotFound  = Error("记录不存在")
	ErrUnderflow = Error("节点记录太少")
)

type Error string

func (e Error) Error() string {
	return string(e)
}

type RecordManager struct {
	fileHandle         *Util.FileHandle
	pageManager        *PageManager
	transactionManager *Transaction.TransactionManager
}

func NewRecordManager(fileHandle *Util.FileHandle) *RecordManager {
	transactionManager := Transaction.NewTransactionManagerWithHandle(fileHandle)
	return &RecordManager{
		fileHandle:         fileHandle,
		pageManager:        NewPageManager(fileHandle),
		transactionManager: transactionManager,
	}
}

// 插入记录
func (rm *RecordManager) InsertRecord(record *Record.Record, tx *Transaction.Transaction) error {
	operation := Transaction.Operation{
		TransactionID: tx.TransactionID,
		OperationType: Transaction.InsertOperation,
		Record:        record,
		OldRecord:     nil,
	}
	rm.transactionManager.AddTransaction(tx)
	rm.transactionManager.AddOperation(operation)

	meta, err := rm.pageManager.GetMetaPage()
	if err != nil {
		return err
	}

	if meta.RootPageID == 0 {
		if err := rm.initBPlusTree(); err != nil {
			return fmt.Errorf("初始化B+树失败: %v", err)
		}
		meta, _ = rm.pageManager.GetMetaPage()
	}

	err, _ = rm.insertRecordToTree(record, meta.RootPageID)
	if err != nil {
		return err
	}
	return nil
}

// 初始化B+树
func (rm *RecordManager) initBPlusTree() error {
	meta, err := rm.pageManager.GetMetaPage()
	if err != nil {
		return err
	}

	// 创建根节点
	rootPage, err := rm.pageManager.CreatePage(Page.LeafPageID)
	if err != nil {
		return err
	}

	rootPage.Header.PageType = Page.LeafPageID

	// 更新元数据
	meta.RootPageID = rootPage.Header.PageID
	meta.FirstPageID = rootPage.Header.PageID
	meta.LastPageID = rootPage.Header.PageID
	meta.TreeHeight = 1

	// 保存元数据更新
	return rm.pageManager.WriteMetaPage()
}

// 递归插入记录
func (rm *RecordManager) insertRecordToTree(record *Record.Record, pageID uint32) (error, *Record.InternalRecord) {
	currentPage, err := rm.pageManager.GetPage(pageID)
	if err != nil {
		return err, nil
	}

	// 如果是内部节点
	if currentPage.Header.PageType == Page.InternalPageID {
		// 找到下一层的页面ID
		nextPageID := rm.findNextPage(currentPage, record.GetKey())
		err, internalRecord := rm.insertRecordToTree(record, nextPageID)

		// 如果下层分裂了，需要处理上升的键
		if err != nil {
			if err.Error() == "页面需要分裂" {
				return rm.handleSplit(currentPage, internalRecord)
			}
		}
		return err, internalRecord
	}

	// 如果是叶子节点
	if currentPage.Header.PageType == Page.LeafPageID {
		// 尝试插入记录
		err = currentPage.InsertRecord(record)
		if err != nil {
			// 如果节点已满，需要分裂
			if err.Error() == "页面已满" {
				internalRecord, err := rm.splitLeafPage(currentPage, record)
				if err != nil {
					return err, internalRecord
				}
				return nil, internalRecord
			}
			return err, nil
		}

		// 更新页面
		return rm.pageManager.UpdatePage(currentPage), nil
	}

	return fmt.Errorf("无效的页面类型"), nil
}

// 递归更新记录
func (rm *RecordManager) updateRecordToTree(record *Record.Record, pageID uint32) (error, *Record.Record, uint32) {
	currentPage, err := rm.pageManager.GetPage(pageID)
	if err != nil {
		return err, nil, 0
	}

	// 如果是内部节点
	if currentPage.Header.PageType == Page.InternalPageID {
		// 找到下一层的页面ID
		nextPageID := rm.findNextPage(currentPage, record.GetKey())
		return rm.updateRecordToTree(record, nextPageID)
	}

	// 如果是叶子节点
	if currentPage.Header.PageType == Page.LeafPageID {
		// 尝试更新记录
		err, oldRecord := currentPage.UpdateRecord(record)
		if err != nil {
			return err, nil, 0
		}
		// 更新页面
		return rm.pageManager.UpdatePage(currentPage), oldRecord, currentPage.Header.PageID
	}

	return fmt.Errorf("无效的页面类型"), nil, 0
}

// 分裂叶子节点
func (rm *RecordManager) splitLeafPage(page *Page.Page, record *Record.Record) (*Record.InternalRecord, error) {
	// 创建新页面
	newPage, err := rm.pageManager.CreatePage(Page.LeafPageID)
	if err != nil {
		return nil, err
	}
	newPage.Header.PageType = Page.LeafPageID

	// 分裂记录
	middleKey, err := page.SplitRecords(newPage)
	if err != nil {
		return nil, err
	}

	// 更新链表指针
	newPage.Header.NextPageID = page.Header.NextPageID
	page.Header.NextPageID = newPage.Header.PageID
	newPage.Header.PrevPageID = page.Header.PageID
	// 根据中间键决定记录应该插入哪个页面
	if bytes.Compare(record.Key[:], middleKey[:]) < 0 {
		err = page.InsertRecord(record)
	} else {
		err = newPage.InsertRecord(record)
	}
	if err != nil {
		return nil, err
	}
	internalRecord := Record.NewInternalRecord(
		*Record.NewRecordHeader(),
		middleKey,
		page.Header.PageID,
		newPage.Header.PageID,
	)
	// 保存更改
	if err := rm.pageManager.UpdatePage(page); err != nil {
		return nil, err
	}
	if err := rm.pageManager.UpdatePage(newPage); err != nil {
		return nil, err
	}

	// 如果是根节点分裂，需要创建新的根节点
	if page.Header.PageID == rm.pageManager.metaPage.RootPageID {
		return nil, rm.createNewRoot(page.Header.PageID, newPage.Header.PageID, middleKey)
	}

	return internalRecord, fmt.Errorf("页面需要分裂")
}

// 创建新的根节点
func (rm *RecordManager) createNewRoot(leftPageID, rightPageID uint32, key [32]byte) error {
	newRoot, err := rm.pageManager.CreatePage(Page.InternalPageID)
	if err != nil {
		return err
	}

	newRoot.Header.PageType = Page.InternalPageID

	// 创建内部记录
	internalRecord := Record.NewInternalRecord(
		*Record.NewRecordHeader(),
		key,
		leftPageID,
		rightPageID,
	)

	// 插入记录到新根节点
	if err := newRoot.InsertInternalRecord(internalRecord); err != nil {
		return err
	}

	// 更新元数据
	meta, err := rm.pageManager.GetMetaPage()
	if err != nil {
		return err
	}

	meta.RootPageID = newRoot.Header.PageID
	meta.TreeHeight++
	if err := rm.pageManager.UpdatePage(newRoot); err != nil {
		return err
	}
	return rm.pageManager.WriteMetaPage()
}

// 在内部节点中查找下一个要访问的页面ID
func (rm *RecordManager) findNextPage(page *Page.Page, key [32]byte) uint32 {
	if page.Header.RecordCount == 0 {
		return 0
	}
	internalRecord := page.FindInternalRecord(key)
	if internalRecord == nil {
		return 0
	}
	if bytes.Compare(internalRecord.Key[:], key[:]) <= 0 {
		return internalRecord.GetNextPointer()
	}
	return internalRecord.GetFrontPointer()
}

// 处理节点分裂
func (rm *RecordManager) handleSplit(page *Page.Page, internalRecord *Record.InternalRecord) (error, *Record.InternalRecord) {
	// 尝试插入内部记录
	err := page.InsertInternalRecord(internalRecord)
	if err != nil {
		// 如果节点已满，需要分裂
		if err.Error() == "页面已满" {
			return rm.splitInternalPage(page, internalRecord)
		}
		return err, nil
	}

	// 更新页面
	return rm.pageManager.UpdatePage(page), nil
}

// 分裂内部节点
func (rm *RecordManager) splitInternalPage(page *Page.Page, record *Record.InternalRecord) (error, *Record.InternalRecord) {
	// 创建新的内部节点页面
	newPage, err := rm.pageManager.CreatePage(Page.InternalPageID)
	if err != nil {
		return err, nil
	}
	newPage.Header.PageType = Page.InternalPageID

	// 分裂记录
	middleKey, err := page.SplitRecords(newPage)
	if err != nil {
		return err, nil
	}

	// 更新页面链接
	newPage.Header.NextPageID = page.Header.NextPageID
	page.Header.NextPageID = newPage.Header.PageID
	newPage.Header.PrevPageID = page.Header.PageID

	// 根据中间键决定记录应该插入哪个页面
	if bytes.Compare(record.Key[:], middleKey[:]) < 0 {
		err = page.InsertInternalRecord(record)
	} else {
		err = newPage.InsertInternalRecord(record)
	}
	if err != nil {
		return err, nil
	}

	// 创建新的内部记录（用于上层节点）
	upRecord := Record.NewInternalRecord(
		*Record.NewRecordHeader(),
		middleKey,
		page.Header.PageID,
		newPage.Header.PageID,
	)

	// 保存更改
	if err := rm.pageManager.UpdatePage(page); err != nil {
		return err, nil
	}
	if err := rm.pageManager.UpdatePage(newPage); err != nil {
		return err, nil
	}

	// 如果是根节点分裂，需要创建新的根节点
	if page.Header.PageID == rm.pageManager.metaPage.RootPageID {
		err := rm.createNewRoot(page.Header.PageID, newPage.Header.PageID, middleKey)
		return err, nil
	}

	// 返回分裂错误和升的记录
	return fmt.Errorf("页面需要分裂"), upRecord
}

// 删除记录
func (rm *RecordManager) DeleteRecord(key [32]byte, tx *Transaction.Transaction) error {
	operation := Transaction.Operation{
		TransactionID: tx.TransactionID,
		OperationType: Transaction.DeleteOperation,
		Record:        nil,
		OldRecord:     nil,
		PageID:        0,
	}
	rm.transactionManager.AddTransaction(tx)
	rm.transactionManager.AddOperation(operation)
	meta, err := rm.pageManager.GetMetaPage()
	if err != nil {
		return err
	}

	if meta.RootPageID == 0 {
		return ErrNotFound
	}

	_, err = rm.deleteRecordFromTree(key, meta.RootPageID, nil)
	if err != nil {
		return err
	}
	return nil
}

type DelResult struct {
	InternalRecord *Record.InternalRecord
}

// 从树中删除记录
func (rm *RecordManager) deleteRecordFromTree(key [32]byte, pageID uint32, internalRecord *Record.InternalRecord) (DelResult, error) {
	currentPage, err := rm.pageManager.GetPage(pageID)
	if err != nil {
		return DelResult{}, err
	}

	// 如果是内部节点
	if currentPage.Header.PageType == Page.InternalPageID {
		nextPageID := rm.findNextPage(currentPage, key)

		result, err := rm.deleteRecordFromTree(key, nextPageID, internalRecord)

		// 如果子节点记录太少，需要重新平衡
		if err != nil {
			if err.Error() == "节点记录太少" {
				return result, rm.handleUnderflow(currentPage)
			}
			return DelResult{}, err
		}

		return DelResult{}, nil
	}

	// 如果是叶子节点
	if currentPage.Header.PageType == Page.LeafPageID {
		// 尝试删除记录
		err := currentPage.DeleteRecord(key)
		// 检查是否需要合并
		if err != nil {
			if err.Error() == "记录不存在" {
				return DelResult{}, ErrNotFound
			} else if err.Error() == "节点记录太少" {
				// 如果是根节点且不为空，允许记录数少于一半
				if currentPage.Header.PageID == rm.pageManager.metaPage.RootPageID &&
					currentPage.Header.RecordCount > 0 {
					return DelResult{}, rm.pageManager.UpdatePage(currentPage)
				}
				//借节点或者是合并节点，借和合并都有可能改变上层的索引值
				err, internalRecord := rm.mergeLeafNodes(currentPage, nil)
				if err != nil {
					return DelResult{}, err
				}
				return DelResult{InternalRecord: internalRecord}, err
			}
			return DelResult{}, err
		}

		//先更新然后再想办法平衡
		rm.pageManager.UpdatePage(currentPage)
		return DelResult{}, fmt.Errorf("节点记录太少")
	}

	return DelResult{}, fmt.Errorf("无效的页面类型")
}

// 更新记录
func (rm *RecordManager) UpdateRecord(record *Record.Record, tx *Transaction.Transaction) error {
	operation := Transaction.Operation{
		TransactionID: tx.TransactionID,
		OperationType: Transaction.UpdateOperation,
		Record:        record,
		OldRecord:     nil,
		PageID:        0,
	}
	rm.transactionManager.AddTransaction(tx)
	rm.transactionManager.AddOperation(operation)
	meta, err := rm.pageManager.GetMetaPage()
	if err != nil {
		return err
	}

	if meta.RootPageID == 0 {
		if err := rm.initBPlusTree(); err != nil {
			return fmt.Errorf("初始化B+树失败: %v", err)
		}
		meta, _ = rm.pageManager.GetMetaPage()
	}

	err, oldRecord, pageID := rm.updateRecordToTree(record, meta.RootPageID)
	if err != nil {
		return err
	}
	operation.OldRecord = oldRecord
	operation.PageID = int32(pageID)
	tx.AddOperation(operation)
	return nil
}

// 合并叶子节点
func (rm *RecordManager) mergeLeafNodes(page *Page.Page, internalRecord *Record.InternalRecord) (error, *Record.InternalRecord) {
	meta, err := rm.pageManager.GetMetaPage()
	if err != nil {
		return err, nil
	}

	// 如果是根节点，且记录数不为0，则不需要合并
	if page.Header.PageID == meta.RootPageID && page.Header.RecordCount > 0 {
		return nil, nil
	}

	// 获取兄弟节点
	isLeft := false
	siblingPage := &Page.Page{}
	if page.Header.PageID == internalRecord.FrontPointer {
		isLeft = false
		siblingPage, err = rm.pageManager.GetPage(internalRecord.NextPointer)
	} else {
		isLeft = true
		siblingPage, err = rm.pageManager.GetPage(internalRecord.FrontPointer)
	}
	//siblingPage, isLeft, err := rm.getSiblingPage(page)
	if err != nil {
		return err, nil
	}

	// 如果可以借用记录
	if siblingPage.Header.RecordCount > siblingPage.Header.MaxRecordCount/2 {
		rm.redistributeRecords(page, siblingPage, isLeft)
		//借用成功之后，范围值改变，需要更新上层索引值
		if internalRecord != nil && isLeft {
			internalRecord.FrontPointer = binary.LittleEndian.Uint32(siblingPage.GetMaxKey())
			internalRecord.NextPointer = binary.LittleEndian.Uint32(page.GetMinKey())
		}

		rm.pageManager.UpdatePage(page)
		rm.pageManager.UpdatePage(siblingPage)
		return nil, internalRecord
	}

	// 否则需要合并节点
	return rm.mergeSiblingPages(page, siblingPage, isLeft), internalRecord
}

// 查找记录
func (rm *RecordManager) FindRecord(key [32]byte) (*Record.Record, error) {
	meta, err := rm.pageManager.GetMetaPage()
	if err != nil {
		return nil, err
	}

	if meta.RootPageID == 0 {
		return nil, ErrNotFound
	}

	return rm.findRecordInTree(key, meta.RootPageID)
}

// 在树中查找记录
func (rm *RecordManager) findRecordInTree(key [32]byte, pageID uint32) (*Record.Record, error) {
	currentPage, err := rm.pageManager.GetPage(pageID)
	if err != nil {
		return nil, err
	}

	// 如果是内部节点
	if currentPage.Header.PageType == Page.InternalPageID {
		nextPageID := rm.findNextPage(currentPage, key)
		return rm.findRecordInTree(key, nextPageID)
	}

	// 如果是叶子节点
	if currentPage.Header.PageType == Page.LeafPageID {
		return currentPage.FindRecord(key)
	}

	return nil, fmt.Errorf("无效的页面类型")
}

// 范围查询
func (rm *RecordManager) RangeQuery(startKey, endKey [32]byte) ([]*Record.Record, error) {
	if rm.pageManager.metaPage == nil {
		return nil, ErrNotFound
	}

	var results []*Record.Record

	// 找到第一个叶子节点
	currentPage, err := rm.findLeafPage(startKey)
	if err != nil {
		return nil, err
	}

	// 遍历叶子节点链表
	for currentPage != nil {
		records, err := currentPage.RangeQuery(startKey, endKey)
		if err != nil {
			return nil, err
		}
		results = append(results, records...)

		// 如果当前页面的最大键大于等于结束键，说明已经找完了
		if bytes.Compare(currentPage.GetMaxKey(), endKey[:]) >= 0 {
			break
		}

		// 获取下一个叶子节点
		currentPage, err = rm.pageManager.GetPage(currentPage.Header.NextPageID)
		if err != nil {
			return nil, err
		}
	}

	return results, nil
}

// 处理节点下溢
func (rm *RecordManager) handleUnderflow(page *Page.Page) error {
	// 如果是根节点，且只有一个子节点，则需要降低树高
	if page.Header.PageID == rm.pageManager.metaPage.RootPageID {
		if page.Header.RecordCount == 0 {
			return rm.decreaseTreeHeight(page)
		}
		return nil
	}

	// 获取兄弟节点
	siblingPage, isLeft, err := rm.getSiblingPage(page)
	if err != nil {
		return err
	}

	// 如果可以借用记录
	if siblingPage.Header.RecordCount > siblingPage.Header.MaxRecordCount/2 {
		return rm.redistributeInternalRecords(page, siblingPage, isLeft)
	}

	// 否则需要合并节点
	return rm.mergeInternalPages(page, siblingPage, isLeft)
}

// 获取兄弟节点
func (rm *RecordManager) getSiblingPage(page *Page.Page) (*Page.Page, bool, error) {
	// 如果有左兄弟节点
	if page.Header.PrevPageID != 0 {
		siblingPage, err := rm.pageManager.GetPage(page.Header.PrevPageID)
		if err != nil {
			return nil, false, err
		}
		return siblingPage, true, nil
	}

	// 如果有右兄弟节点
	if page.Header.NextPageID != 0 {
		siblingPage, err := rm.pageManager.GetPage(page.Header.NextPageID)
		if err != nil {
			return nil, false, err
		}
		return siblingPage, false, nil
	}

	return nil, false, fmt.Errorf("节点没有兄弟节点")
}

// 重新分配记录（叶子节点）
func (rm *RecordManager) redistributeRecords(page, siblingPage *Page.Page, isLeft bool) error {
	if isLeft {
		// 从左兄弟节点借一个记录
		record, err := siblingPage.RemoveLastRecord()
		if err != nil {
			return err
		}
		return page.InsertRecord(record)
	} else {
		// 从右兄弟节点借一个记录
		record, err := siblingPage.RemoveFirstRecord()
		if err != nil {
			return err
		}
		return page.InsertRecord(record)
	}
}

// 重新分配记录（内部节点）
func (rm *RecordManager) redistributeInternalRecords(page, siblingPage *Page.Page, isLeft bool) error {
	if isLeft {
		// 从左兄弟节点借一个记录
		record, err := siblingPage.RemoveLastInternalRecord()
		if err != nil {
			return err
		}
		return page.InsertInternalRecord(record)
	} else {
		// 从右兄弟节点借一个记录
		record, err := siblingPage.RemoveFirstInternalRecord()
		if err != nil {
			return err
		}
		return page.InsertInternalRecord(record)
	}
}

// 合并叶子节点
func (rm *RecordManager) mergeSiblingPages(page, siblingPage *Page.Page, isLeft bool) error {
	var targetPage, sourcePage *Page.Page
	if isLeft {
		targetPage = siblingPage
		sourcePage = page
	} else {
		targetPage = page
		sourcePage = siblingPage
	}

	// 将源页面的记录复制到目标页面
	records, err := sourcePage.GetAllRecords()
	if err != nil {
		return err
	}

	for _, record := range records {
		if err := targetPage.InsertRecord(record); err != nil {
			return err
		}
	}

	// 更新链表指针
	targetPage.Header.NextPageID = sourcePage.Header.NextPageID
	if sourcePage.Header.NextPageID != 0 {
		nextPage, err := rm.pageManager.GetPage(sourcePage.Header.NextPageID)
		if err != nil {
			return err
		}
		nextPage.Header.PrevPageID = targetPage.Header.PageID
		if err := rm.pageManager.UpdatePage(nextPage); err != nil {
			return err
		}
	}

	// 更新页面
	if err := rm.pageManager.UpdatePage(targetPage); err != nil {
		return err
	}

	// 删除源页面
	return rm.pageManager.DisposePage(sourcePage)
}

// 降低树的高度
func (rm *RecordManager) decreaseTreeHeight(rootPage *Page.Page) error {
	meta, err := rm.pageManager.GetMetaPage()
	if err != nil {
		return err
	}

	childRecord := rootPage.GetFirstInternalRecord()
	if childRecord == nil {
		return fmt.Errorf("根节点没有子节点")
	}

	childPage, err := rm.pageManager.GetPage(childRecord.GetFrontPointer())
	if err != nil {
		return err
	}

	// 更新元数据
	meta.RootPageID = childPage.Header.PageID
	meta.TreeHeight--

	if err := rm.pageManager.WriteMetaPage(); err != nil {
		return err
	}

	return rm.pageManager.DisposePage(rootPage)
}

// 查找叶子节点
func (rm *RecordManager) findLeafPage(key [32]byte) (*Page.Page, error) {
	if rm.pageManager.metaPage == nil {
		return nil, ErrNotFound
	}

	currentPageID := rm.pageManager.metaPage.RootPageID
	for {
		currentPage, err := rm.pageManager.GetPage(currentPageID)
		if err != nil {
			return nil, err
		}

		if currentPage.Header.PageType == Page.LeafPageID {
			return currentPage, nil
		}

		currentPageID = rm.findNextPage(currentPage, key)
	}
}

// 合并内部节点
func (rm *RecordManager) mergeInternalPages(page, siblingPage *Page.Page, isLeft bool) error {
	var targetPage, sourcePage *Page.Page
	if isLeft {
		targetPage = siblingPage
		sourcePage = page
	} else {
		targetPage = page
		sourcePage = siblingPage
	}

	// 将源页面的记录复制到目标页面
	for i := uint32(0); i < sourcePage.Header.RecordCount; i++ {
		record := sourcePage.GetInternalRecord(int(i))
		if err := targetPage.InsertInternalRecord(record); err != nil {
			return err
		}
	}

	// 更新页面链接
	targetPage.Header.NextPageID = sourcePage.Header.NextPageID
	if sourcePage.Header.NextPageID != 0 {
		nextPage, err := rm.pageManager.GetPage(sourcePage.Header.NextPageID)
		if err != nil {
			return err
		}
		nextPage.Header.PrevPageID = targetPage.Header.PageID
		if err := rm.pageManager.UpdatePage(nextPage); err != nil {
			return err
		}
	}

	// 更新页面
	if err := rm.pageManager.UpdatePage(targetPage); err != nil {
		return err
	}

	// 删除源页面
	return rm.pageManager.DisposePage(sourcePage)
}

// 回滚事务
func (rm *RecordManager) Rollback(transaction *Transaction.Transaction) error {
	for i := len(transaction.Operations) - 1; i >= 0; i-- {
		operation := transaction.Operations[i]
		if operation.OperationType == Transaction.UpdateOperation {
			_, _, pageID := rm.updateRecordToTree(operation.OldRecord, uint32(operation.PageID))
			operation.PageID = int32(pageID)
		}
		if operation.OperationType == Transaction.DeleteOperation {
			rm.InsertRecord(operation.Record, transaction)
		}
		if operation.OperationType == Transaction.InsertOperation {
			rm.DeleteRecord(operation.Record.Key, transaction)
		}
	}
	rm.transactionManager.Rollback(transaction.TransactionID)
	return nil
}

// 撤销事务
func (rm *RecordManager) Undo(transaction *Transaction.Transaction) error {
	operation := transaction.Operations[len(transaction.Operations)-1]
	if operation.OperationType == Transaction.UpdateOperation {
		_, _, pageID := rm.updateRecordToTree(operation.OldRecord, uint32(operation.PageID))
		operation.PageID = int32(pageID)
	}
	if operation.OperationType == Transaction.DeleteOperation {
		rm.InsertRecord(operation.Record, transaction)
	}
	if operation.OperationType == Transaction.InsertOperation {
		rm.DeleteRecord(operation.Record.Key, transaction)
	}
	rm.transactionManager.Undo(transaction.TransactionID)
	return nil
}

// TreeReverse 遍历并输出B+树的结构
func (rm *RecordManager) TreeReverse() error {
	meta, err := rm.pageManager.GetMetaPage()
	if err != nil {
		return err
	}

	if meta.RootPageID == 0 {
		fmt.Println("空树")
		return nil
	}

	// 使用队列进行层次遍历
	type QueueItem struct {
		pageID uint32
		level  int
	}
	queue := []QueueItem{{pageID: meta.RootPageID, level: 1}}
	currentLevel := 1

	fmt.Printf("B+树结构 (高度: %d)\n", meta.TreeHeight)
	fmt.Println("====================")

	for len(queue) > 0 {
		// 出队
		item := queue[0]
		queue = queue[1:]

		// 如果进入新的层级，打印层级信息
		if item.level > currentLevel {
			fmt.Println("--------------------")
			currentLevel = item.level
			fmt.Printf("第 %d 层:\n", currentLevel)
		}

		// 获取页面
		page, err := rm.pageManager.GetPage(item.pageID)
		if err != nil {
			return fmt.Errorf("获取页面失败 (ID=%d): %v", item.pageID, err)
		}

		// 输出页面信息
		fmt.Printf("页面ID: %d, 类型: %s, 记录数: %d",
			page.Header.PageID,
			getPageTypeName(page.Header.PageType),
			page.Header.RecordCount)

		// 如果是叶子节点，输出前后指针
		if page.Header.PageType == Page.LeafPageID {
			fmt.Printf(", Prev: %d, Next: %d",
				page.Header.PrevPageID,
				page.Header.NextPageID)
		}
		fmt.Println()

		// 输出键值
		fmt.Print("键值: [")
		for i := uint32(0); i < page.Header.RecordCount; i++ {
			key, err := page.ReadKey(i*32, 32)
			if err != nil {
				return fmt.Errorf("读取键值失败: %v", err)
			}
			if i > 0 {
				fmt.Print(", ")
			}
			fmt.Printf("%d", key[3]) // 假设键值在最后一个字节
		}
		fmt.Println("]")

		// 如果是内部节点，将子节点加入队列
		if page.Header.PageType == Page.InternalPageID {
			for i := uint32(0); i < page.Header.RecordCount; i++ {
				internalRecord := page.GetInternalRecord(int(i))
				// 将前向指针加入队列
				queue = append(queue, QueueItem{
					pageID: internalRecord.GetFrontPointer(),
					level:  item.level + 1,
				})
				// 最后一个记录还需要加入后向指针
				if i == page.Header.RecordCount-1 {
					queue = append(queue, QueueItem{
						pageID: internalRecord.GetNextPointer(),
						level:  item.level + 1,
					})
				}
			}
		}
	}

	fmt.Println("====================")
	return nil
}

// 获取页面类型的字符串表示
func getPageTypeName(pageType uint32) string {
	switch pageType {
	case Page.MetaPageID:
		return "Meta"
	case Page.InternalPageID:
		return "Internal"
	case Page.LeafPageID:
		return "Leaf"
	default:
		return "Unknown"
	}
}
