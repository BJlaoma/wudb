package manager

import (
	"bytes"
	"testing"
	"time"
	"wudb/Entity/Record"
)

// 测试环境设置
func setupRecordManagerTest(t *testing.T) (*RecordManager, *FileManager, func()) {
	// 创建文件管理器
	fm := &FileManager{}
	testFile := "test_record_manager"

	// 创建测试文件
	if err := fm.CreateFile(testFile); err != nil {
		t.Fatalf("创建测试文件失败: %v", err)
	}

	// 打开文件
	handle, err := fm.OpenFile(testFile)
	if err != nil {
		t.Fatalf("打开文件失败: %v", err)
	}

	// 创建记录管理器
	rm := NewRecordManager(handle)

	// 返回清理函数
	cleanup := func() {
		handle.Close()
		fm.DestroyFile(testFile)
	}

	return rm, fm, cleanup
}

// 创建测试记录
func createTestRecord(key uint32, value string) *Record.Record {
	var keyBytes [32]byte
	keyBytes[0] = byte(key >> 24)
	keyBytes[1] = byte(key >> 16)
	keyBytes[2] = byte(key >> 8)
	keyBytes[3] = byte(key)

	var valueBytes [128]byte
	copy(valueBytes[:], value)

	return Record.NewRecord(
		Record.RecordHeader{
			IsDeleted:     0,
			RecordLength:  uint32(32 + len(value)),
			KeySize:       32,
			ValueSize:     uint32(len(value)),
			TransactionID: 0,
			Timestamp:     uint32(time.Now().Unix()),
		},
		keyBytes,
		valueBytes,
	)
}

// 测试插入单条记录
func TestRecordManager_InsertSingleRecord(t *testing.T) {
	rm, _, cleanup := setupRecordManagerTest(t)
	defer cleanup()

	t.Log("开始测试插入单条记录")

	// 创建测试记录
	record := createTestRecord(1, "test value")

	// 插入记录
	err := rm.InsertRecord(record)
	if err != nil {
		t.Fatalf("插入记录失败: %v", err)
	}

	// 验证记录是否存在
	found, err := rm.FindRecord(record.GetKey())
	if err != nil {
		t.Fatalf("查找记录失败: %v", err)
	}
	if found == nil {
		t.Error("未找到插入的记录")
	}
	if !bytes.Equal(found.Value[:len("test value")], []byte("test value")) {
		t.Error("记录值不匹配")
	}
}

// 测试插入多条记录
func TestRecordManager_InsertMultipleRecords(t *testing.T) {
	rm, _, cleanup := setupRecordManagerTest(t)
	defer cleanup()

	t.Log("开始测试插入多条记录")

	// 插入多条记录
	recordCount := 100
	for i := 0; i < recordCount; i++ {
		record := createTestRecord(uint32(i), "value"+string(rune(i)))
		if err := rm.InsertRecord(record); err != nil {
			t.Fatalf("插入第 %d 条记录失败: %v", i, err)
		}
	}

	// 验证所有记录
	for i := 0; i < recordCount; i++ {
		var key [32]byte
		key[0] = byte(i >> 24)
		key[1] = byte(i >> 16)
		key[2] = byte(i >> 8)
		key[3] = byte(i)

		found, err := rm.FindRecord(key)
		if err != nil {
			t.Errorf("查找第 %d 条记录失败: %v", i, err)
			continue
		}
		if found == nil {
			t.Errorf("未找到第 %d 条记录", i)
		}
	}
}

// 测试页面分裂
func TestRecordManager_PageSplit(t *testing.T) {
	rm, _, cleanup := setupRecordManagerTest(t)
	defer cleanup()

	t.Log("开始测试页面分裂")

	// 插入足够多的记录以触发分裂
	recordCount := 200 // 应该足够触发分裂
	for i := 0; i < recordCount; i++ {
		record := createTestRecord(uint32(i), "test value "+string(rune(i)))
		if err := rm.InsertRecord(record); err != nil {
			t.Fatalf("插入第 %d 条记录失败: %v", i, err)
		}
	}

	// 验证B+树高度
	if rm.pageManager.metaPage.TreeHeight < 2 {
		t.Error("页面没有正确分裂，树高度应该大于1")
	}

	// 验证所有记录是否都能找到
	for i := 0; i < recordCount; i++ {
		var key [32]byte
		key[0] = byte(i >> 24)
		key[1] = byte(i >> 16)
		key[2] = byte(i >> 8)
		key[3] = byte(i)

		if _, err := rm.FindRecord(key); err != nil {
			t.Errorf("查找第 %d 条记录失败: %v", i, err)
		}
	}
}

// 测试删除记录
func TestRecordManager_DeleteRecord(t *testing.T) {
	rm, _, cleanup := setupRecordManagerTest(t)
	defer cleanup()

	t.Log("开始测试删除记录")

	// 先插入一些记录
	records := make([]*Record.Record, 20)
	for i := 0; i < 20; i++ {
		records[i] = createTestRecord(uint32(i), "test value "+string(rune(i)))
		if err := rm.InsertRecord(records[i]); err != nil {
			t.Fatalf("插入记录失败: %v", err)
		}
	}

	// 删除部分记录
	for i := 0; i < 10; i++ {
		if err := rm.DeleteRecord(records[i].GetKey()); err != nil {
			t.Errorf("删除记录失败: %v", err)
		}
	}

	// 验证删除的记录不存在
	for i := 0; i < 10; i++ {
		_, err := rm.FindRecord(records[i].GetKey())
		if err == nil {
			t.Errorf("记录 %d 应该已被删除", i)
		}
	}

	// 验证未删除的记录仍然存在
	for i := 10; i < 20; i++ {
		found, err := rm.FindRecord(records[i].GetKey())
		if err != nil {
			t.Errorf("查找记录 %d 失败: %v", i, err)
		}
		if found == nil {
			t.Errorf("记录 %d 应该存在", i)
		}
	}
}

// 测试范围查询
func TestRecordManager_RangeQuery(t *testing.T) {
	rm, _, cleanup := setupRecordManagerTest(t)
	defer cleanup()

	t.Log("开始测试范围查询")

	// 插入有序记录
	recordCount := 100
	for i := 0; i < recordCount; i++ {
		record := createTestRecord(uint32(i), "value"+string(rune(i)))
		if err := rm.InsertRecord(record); err != nil {
			t.Fatalf("插入记录失败: %v", err)
		}
	}

	// 定义范围
	var startKey, endKey [32]byte
	startKey[3] = 20
	endKey[3] = 50

	// 执行范围查询
	results, err := rm.RangeQuery(startKey, endKey)
	if err != nil {
		t.Fatalf("范围查询失败: %v", err)
	}

	// 验证结果数量
	expectedCount := 31 // 20到50之间有31个数
	if len(results) != expectedCount {
		t.Errorf("范围查询结果数量不正确: 期望 %d, 实际 %d", expectedCount, len(results))
	}

	// 验证结果有序性
	for i := 1; i < len(results); i++ {
		if bytes.Compare(results[i-1].Key[:], results[i].Key[:]) >= 0 {
			t.Error("范围查询结果未正确排序")
		}
	}
}
