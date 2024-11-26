package Page

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"
	"unsafe"
)

// 大小 64Byte
type PageHeader struct {
	PageType   uint32 // 页类型
	PageID     uint32 // 页ID
	PrevPageID uint32 // 上一页ID
	NextPageID uint32 // 下一页ID

	LSN uint32 // 日志序列号

	FreeSpaceStart uint32  // 空闲空间起始位置
	FreeSpaceEnd   uint32  // 空闲空间结束位置
	RecordCount    uint32  // 记录数
	CheckSum       uint32  // 校验和
	RecordSize     uint32  // 记录的占用大小
	MaxRecordCount uint32  // 最大记录数
	IsDirty        uint8   // 是否脏页
	Reserved1      [3]byte // 保留字段1

	TransactionID uint32 // 事务ID
	CreateTime    uint32 // 创建时间
	ModifyTime    uint32 // 修改时间
	RecordID      uint32 // 记录ID
}

const (
	PageHeaderSize = unsafe.Sizeof(PageHeader{}) // 实际大小
	MetaPageID     = 0
	InternalPageID = 1
	LeafPageID     = 2
)

func init() {
	fmt.Printf("PageHeader 大小: %d 字节\n", PageHeaderSize)
}

func NewPageHeader() *PageHeader {
	fmt.Printf("PageHeader 大小: %d 字节\n", PageHeaderSize)
	CreateTime := time.Now().Unix()
	ModifyTime := CreateTime

	return &PageHeader{
		CreateTime: uint32(CreateTime),
		ModifyTime: uint32(ModifyTime),
	}
}

func (ph *PageHeader) SerializeTo() ([]byte, error) {
	buffer := new(bytes.Buffer)
	err := binary.Write(buffer, binary.LittleEndian, ph)
	if err != nil {
		return nil, fmt.Errorf("序列化失败: %v", err)
	}
	return buffer.Bytes(), nil
}

// 反序列化
func (ph *PageHeader) DeserializeFrom(data []byte) error {
	if len(data) < int(PageHeaderSize) {
		return fmt.Errorf("数据长度不足: 期望 %d 字节, 实际 %d 字节", PageHeaderSize, len(data))
	}

	// 直接反序列化整个结构体
	return binary.Read(bytes.NewReader(data), binary.LittleEndian, ph)
}
