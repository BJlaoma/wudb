package Page

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

/*
页面物理布局：

Meta Page (第0页):
+------------------------+ <- 0
|      PageHeader        |
+------------------------+ <- 64
|    BTree Meta Info     |
|  - Root PageID         |
|  - First Leaf PageID   |
|  - Last Leaf PageID    |
|  - Tree Height         |
+------------------------+

Internal Node Page:
+------------------------+ <- 0
|      PageHeader        |
+------------------------+ <- 64
|     Key Array          |
+------------------------+
|    Children Array      |
+------------------------+ <- FreeSpaceStart
|     Free Space         |
+------------------------+ <- PageSize-SlotArraySize
|     Slot Array         |

Leaf Node Page:
+------------------------+ <- 0
|      PageHeader        |
+------------------------+ <- 64
|     Key Array          |
+------------------------+
|    Record Array        |
+------------------------+ <- FreeSpaceStart
|     Free Space         |
+------------------------+ <- PageSize-SlotArraySize
|     Slot Array         |
*/
// 页大小 4KB = 4096 byte
const PageSize = 4096

type Page struct {
	Header PageHeader
	Key    [512]byte
	Value  [3520]byte
}

const (
	KeyMaxSize = 64
)

func NewPage() *Page {
	header := NewPageHeader()
	return &Page{
		Header: *header,
	}
}

func (p *Page) GetHeader() *PageHeader {
	return &p.Header
}

// 序列化页面为字节数组
func (p *Page) SerializeTo() ([]byte, error) {
	buffer := make([]byte, PageSize) // 4096字节
	buf := bytes.NewBuffer(buffer[:0])
	buf.Reset() // 清空buffer
	// 直接序列化整个Page结构体
	if err := binary.Write(buf, binary.LittleEndian, p); err != nil {
		return nil, fmt.Errorf("序列化页面失败: %v", err)
	}

	return buffer, nil
}

// 从字节数组反序列化页面
func (p *Page) DeserializeFrom(data []byte) error {
	if len(data) != PageSize {
		return fmt.Errorf("数据大小错误: 期望 %d 字节, 实际 %d 字节", PageSize, len(data))
	}

	// 反序列化页头
	if err := p.Header.DeserializeFrom(data[:PageHeaderSize]); err != nil {
		return fmt.Errorf("反序列化页头失败: %v", err)
	}

	// 复制数据区
	copy(p.Key[:], data[PageHeaderSize:])
	copy(p.Value[:], data[PageHeaderSize+512:])

	return nil
}

// 写入数据到页面key
func (p *Page) WriteKey(offset uint32, data []byte) error {
	if offset+uint32(len(data)) > uint32(len(p.Key)) {
		return fmt.Errorf("写入超出页面大小: offset=%d, len=%d, maxSize=%d",
			offset, len(data), len(p.Key))
	}

	copy(p.Key[offset:], data)
	return nil
}

// 从页面读取数据
func (p *Page) ReadKey(offset uint32, length uint32) ([]byte, error) {
	if offset+length > uint32(len(p.Key)) {
		return nil, fmt.Errorf("读取超出页面大小: offset=%d, len=%d, maxSize=%d",
			offset, length, len(p.Key))
	}

	result := make([]byte, length)
	copy(result, p.Key[offset:offset+length])
	return result, nil
}

func (p *Page) WriteValue(offset uint32, data []byte) error {
	if offset+uint32(len(data)) > uint32(len(p.Value)) {
		return fmt.Errorf("写入超出页面大小: offset=%d, len=%d, maxSize=%d",
			offset, len(data), len(p.Value))
	}
	copy(p.Value[offset:], data)
	return nil
}

func (p *Page) ReadValue(offset uint32, length uint32) ([]byte, error) {
	if offset+length > uint32(len(p.Value)) {
		return nil, fmt.Errorf("读取超出页面大小: offset=%d, len=%d, maxSize=%d",
			offset, length, len(p.Value))
	}
	result := make([]byte, length)
	copy(result, p.Value[offset:offset+length])
	return result, nil
}
