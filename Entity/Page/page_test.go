package Page

import (
	"bytes"
	"testing"
	"wudb/Entity/Record"
)

func TestPage_InsertAndFindRecord(t *testing.T) {
	page := NewPage()
	page.Header.PageType = LeafPageID

	// 创建测试记录
	testKey := [32]byte{1, 2, 3}
	testValue := [128]byte{4, 5, 6}
	record := Record.NewRecord(Record.RecordHeader{}, testKey, testValue)

	// 测试插入
	err := page.InsertRecord(record)
	if err != nil {
		t.Fatalf("插入记录失败: %v", err)
	}

	// 测试查找
	found, err := page.FindRecord(testKey)
	if err != nil {
		t.Fatalf("查找记录失败: %v", err)
	}
	if found == nil {
		t.Fatal("未找到插入的记录")
	}
	if !bytes.Equal(found.Key[:], testKey[:]) {
		t.Error("记录键不匹配")
	}
	if !bytes.Equal(found.Value[:], testValue[:]) {
		t.Error("记录值不匹配")
	}
}

func TestPage_SplitRecords(t *testing.T) {
	page := NewPage()
	page.Header.PageType = LeafPageID
	newPage := NewPage()
	newPage.Header.PageType = LeafPageID

	// 插入多条记录
	for i := byte(0); i < 10; i++ {
		key := [32]byte{i}
		value := [128]byte{i}
		record := Record.NewRecord(Record.RecordHeader{}, key, value)
		if err := page.InsertRecord(record); err != nil {
			t.Fatalf("插入记录失败: %v", err)
		}
	}

	// 测试分裂
	middleKey, err := page.SplitRecords(newPage)
	if err != nil {
		t.Fatalf("分裂记录失败: %v", err)
	}

	// 验证分裂结果
	if page.Header.RecordCount != 5 || newPage.Header.RecordCount != 5 {
		t.Error("分裂后记录数不正确")
	}

	// 验证中间键
	if middleKey[0] != 5 {
		t.Error("中间键不正确")
	}
}
