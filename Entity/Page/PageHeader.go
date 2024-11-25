package Page

import (
	"fmt"
	"unsafe"
)

// 大小 10*32+8 =40 +1 byte = 41byte
type PageHeader struct {
	PageID         uint32
	PrevPageID     uint32
	NextPageID     uint32 // 下一页ID
	LSN            uint32 // 日志序列号
	TransactionID  uint32 // 事务ID
	FreeSpaceStart uint32 // 空闲空间起始位置
	FreeSpaceEnd   uint32 // 空闲空间结束位置
	IsDirty        bool   // 是否脏页
	RecordCount    uint32 // 记录数
	CheckSum       uint32 // 校验和
	RecordSize     uint32 // 记录的占用大小
}

const (
	PageHeaderSize = unsafe.Sizeof(PageHeader{}) // 实际大小
)

func init() {
	fmt.Printf("PageHeader 大小: %d 字节\n", PageHeaderSize)
}
