package Page

import (
	"fmt"
	"testing"
	"time"
)

func TestPageHeader(t *testing.T) {
	header := NewPageHeader()
	fmt.Println(header)
}

func TestPageHeader_Serialization(t *testing.T) {
	header := &PageHeader{
		PageID:     1,
		CreateTime: uint32(time.Now().Unix()),
		ModifyTime: uint32(time.Now().Unix()),
	}

	// 序列化
	data, err := header.SerializeTo()
	if err != nil {
		t.Fatalf("序列化失败: %v", err)
	}

	// 验证数据不全为0
	allZero := true
	for _, b := range data {
		if b != 0 {
			allZero = false
			break
		}
	}
	if allZero {
		t.Error("序列化数据全为0")
	}

	// 反序列化并验证
	newHeader := &PageHeader{}
	if err := newHeader.DeserializeFrom(data); err != nil {
		t.Fatalf("反序列化失败: %v", err)
	}

	// 验证字段值
	if newHeader.PageID != header.PageID {
		t.Errorf("PageID不匹配: 期望 %d, 实际 %d", header.PageID, newHeader.PageID)
	}
}
