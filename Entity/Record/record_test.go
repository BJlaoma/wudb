package Record

import (
	"bytes"
	"testing"
)

func TestRecord_SerializeAndDeserialize(t *testing.T) {
	// 创建测试记录
	key := [32]byte{1, 2, 3}
	value := [128]byte{4, 5, 6}
	record := NewRecord(RecordHeader{
		IsDeleted:     0,
		RecordLength:  160, // 32 + 128
		KeySize:       32,
		ValueSize:     128,
		TransactionID: 1,
	}, key, value)

	// 测试序列化
	data, err := record.SerializeTo()
	if err != nil {
		t.Fatalf("序列化失败: %v", err)
	}

	// 测试反序列化
	newRecord := &Record{}
	if err := newRecord.DeserializeFrom(data); err != nil {
		t.Fatalf("反序列化失败: %v", err)
	}

	// 验证结果
	if !bytes.Equal(newRecord.Key[:], key[:]) {
		t.Error("键不匹配")
	}
	if !bytes.Equal(newRecord.Value[:], value[:]) {
		t.Error("值不匹配")
	}
	if newRecord.Header.RecordLength != 160 {
		t.Error("记录长度不正确")
	}
}

func TestInternalRecord_SerializeAndDeserialize(t *testing.T) {
	// 创建测试内部记录
	key := [32]byte{1, 2, 3}
	record := NewInternalRecord(RecordHeader{
		IsDeleted:     0,
		RecordLength:  192, // 32 + 128 + 32
		KeySize:       32,
		ValueSize:     128,
		TransactionID: 1,
	}, key, 1, 2)

	// 测试序列化
	data, err := record.SerializeTo()
	if err != nil {
		t.Fatalf("序列化失败: %v", err)
	}

	// 测试反序列化
	newRecord := &InternalRecord{}
	if err := newRecord.DeserializeFrom(data); err != nil {
		t.Fatalf("反序列化失败: %v", err)
	}

	// 验证结果
	if !bytes.Equal(newRecord.Key[:], key[:]) {
		t.Error("键不匹配")
	}
	if newRecord.FrontPointer != 1 {
		t.Error("前向指针不正确")
	}
	if newRecord.NextPointer != 2 {
		t.Error("后向指针不正确")
	}
}
