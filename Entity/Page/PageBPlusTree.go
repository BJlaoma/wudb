package Page

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// size 128B
type PageBPlusTree struct {
	Header      PageHeader // 64Byte
	RootPageID  uint32
	FirstPageID uint32
	LastPageID  uint32
	PageCount   uint32
	TreeHeight  uint32
	Reserved    [4012]byte
}

type InternalPage struct {
	page *Page
}

type LeafPage struct {
	page *Page
}

func NewPageBPlusTree() *PageBPlusTree {
	meta := &PageBPlusTree{}
	meta.Header.PageType = MetaPageID
	meta.Header.PageID = 0 // MetaPage总是第0页
	return meta
}

func (p *PageBPlusTree) SerializeTo() ([]byte, error) {

	buffer := make([]byte, PageSize)
	buf := bytes.NewBuffer(buffer[:0])
	buf.Reset() // 清空buffer

	if err := binary.Write(buf, binary.LittleEndian, p); err != nil {
		return nil, fmt.Errorf("序列化失败: %v", err)
	}
	return buffer, nil
}

func (p *PageBPlusTree) DeserializeFrom(data []byte) error {
	if len(data) != int(PageSize) {
		return fmt.Errorf("数据大小错误: 期望 %d 字节, 实际 %d 字节", PageSize, len(data))
	}
	return p.Header.DeserializeFrom(data[:PageHeaderSize])
}

func (p *PageBPlusTree) GetRootPageID() uint32 {
	return p.RootPageID
}

func (p *PageBPlusTree) GetFirstLeafPageID() uint32 {
	return p.FirstPageID
}

func (p *PageBPlusTree) GetLastLeafPageID() uint32 {
	return p.LastPageID
}

func (p *PageBPlusTree) GetTreeHeight() uint32 {
	return p.TreeHeight
}

func (p *PageBPlusTree) GetPageCount() uint32 {
	return p.PageCount
}

func (p *PageBPlusTree) GetReserved() [4012]byte {
	return p.Reserved
}

func (p *PageBPlusTree) SetRootPageID(rootPageID uint32) {
	p.RootPageID = rootPageID
}

func (p *PageBPlusTree) SetFirstLeafPageID(firstPageID uint32) {
	p.FirstPageID = firstPageID
}

func (p *PageBPlusTree) SetLastLeafPageID(lastPageID uint32) {
	p.LastPageID = lastPageID
}

func (p *PageBPlusTree) SetTreeHeight(treeHeight uint32) {
	p.TreeHeight = treeHeight
}

func (p *PageBPlusTree) SetPageCount(pageCount uint32) {
	p.PageCount = pageCount
}
