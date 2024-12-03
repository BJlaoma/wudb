package Page

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"wudb/Entity/Record"
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

// 内部节点有关

// 查找内部节点记录
func (p *Page) FindInternalRecord(recordKey [32]byte) *Record.InternalRecord {
	// 返回值：
	// -1 表示 key1 < key2
	//  0 表示 key1 = key2
	//  1 表示 key1 > key2
	if bytes.Compare(p.Key[:32], recordKey[:]) > 0 {
		return p.GetInternalRecord(0)
	}
	if p.Header.RecordCount == 0 {
		return nil
	}
	if p.Header.RecordCount == 1 {
		return p.GetInternalRecord(0)
	}
	for i := 0; i < int(p.Header.RecordCount)-1; i++ {
		if bytes.Compare(p.Key[i*32:(i+1)*32], recordKey[:]) <= 0 && bytes.Compare(p.Key[(i+1)*32:(i+2)*32], recordKey[:]) > 0 {
			return p.GetInternalRecord(i)
		}
	}

	return p.GetInternalRecord(int(p.Header.RecordCount) - 1)
}

func (p *Page) GetInternalRecord(id int) *Record.InternalRecord {
	offset := id * int(Record.InternalRecordSize)
	record := &Record.InternalRecord{}
	record.DeserializeFrom(p.Value[offset : offset+int(Record.InternalRecordSize)])
	return record
}

// 叶子节点有关

// 插入记录到叶子节点
func (p *Page) InsertRecord(record *Record.Record) error {
	if p.Header.RecordCount >= p.Header.MaxRecordCount {
		return fmt.Errorf("页面已满")
	}

	// 1. 找到插入位置
	insertPos := 0
	recordKey := record.GetKey()

	// 二分查找插入位置
	left, right := 0, int(p.Header.RecordCount)-1
	for left <= right {
		mid := (left + right) / 2
		// 读取mid位置的key
		currentKey, err := p.ReadKey(uint32(mid*32), 32)
		if err != nil {
			return fmt.Errorf("读取key失败: %v", err)
		}

		cmp := bytes.Compare(recordKey[:], currentKey[:])
		if cmp == 0 {
			return fmt.Errorf("key已存在")
		} else if cmp < 0 {
			right = mid - 1
		} else {
			left = mid + 1
		}
	}
	insertPos = left

	// 2. 移动现有记录，为新记录腾出空间
	if insertPos < int(p.Header.RecordCount) {
		// 移动key
		for i := int(p.Header.RecordCount); i > insertPos; i-- {
			key, err := p.ReadKey(uint32((i-1)*32), 32)
			if err != nil {
				return fmt.Errorf("读取key失败: %v", err)
			}
			if err := p.WriteKey(uint32(i*32), key); err != nil {
				return fmt.Errorf("写入key失败: %v", err)
			}
		}

		// 移动value
		for i := int(p.Header.RecordCount); i > insertPos; i-- {
			value, err := p.ReadValue(uint32((uint32(i-1))*record.GetRecordSize()), uint32(record.GetRecordSize()))
			if err != nil {
				return fmt.Errorf("读取value失败: %v", err)
			}
			if err := p.WriteValue(uint32((uint32(i) * record.GetRecordSize())), value); err != nil {
				return fmt.Errorf("写入value失败: %v", err)
			}
		}
	}

	// 3. 写入新记录
	if err := p.WriteKey(uint32(insertPos*32), recordKey[:]); err != nil {
		return fmt.Errorf("写入key失败: %v", err)
	}

	recordValue, err := record.SerializeTo()
	if err != nil {
		return fmt.Errorf("序列化记录失败: %v", err)
	}

	if err := p.WriteValue(uint32((uint32(insertPos) * record.GetRecordSize())), recordValue[:]); err != nil {
		return fmt.Errorf("写入value失败: %v", err)
	}

	// 4. 更新记录数
	p.Header.RecordCount++

	return nil
}

// 分裂叶子节点
func (p *Page) SplitKeyAndValue() ([]byte, []byte, [32]byte) {
	splitPos := p.Header.RecordCount / 2

	// 获取需要分割的数据
	key := make([]byte, (p.Header.RecordCount-splitPos)*32)
	value := make([]byte, (p.Header.RecordCount-splitPos)*p.Header.RecordSize)

	// 复制要分割的数据
	copy(key, p.Key[splitPos*32:(p.Header.RecordCount)*32])
	copy(value, p.Value[splitPos*p.Header.RecordSize:(p.Header.RecordCount)*p.Header.RecordSize])

	// 清空原页面中分割出去的数据
	for i := splitPos * 32; i < p.Header.RecordCount*32; i++ {
		p.Key[i] = 0
	}
	for i := splitPos * p.Header.RecordSize; i < p.Header.RecordCount*p.Header.RecordSize; i++ {
		p.Value[i] = 0
	}

	// 更新记录数
	p.Header.RecordCount = splitPos
	recordKey := [32]byte{}
	copy(recordKey[:], key[0:32])
	return key, value, recordKey
}

// 分裂记录并返回中间键
func (p *Page) SplitRecords(newPage *Page) ([32]byte, error) {
	// 计算中间位置
	midIndex := p.Header.RecordCount / 2

	// 复制后半部分记录到新页面
	for i := midIndex; i < p.Header.RecordCount; i++ {
		key := p.Key[i*32 : (i+1)*32]
		value := p.Value[i*p.Header.RecordSize : (i+1)*p.Header.RecordSize]

		if err := newPage.WriteKey(uint32((i-midIndex)*32), key); err != nil {
			return [32]byte{}, err
		}
		if err := newPage.WriteValue(uint32((i-midIndex)*p.Header.RecordSize), value); err != nil {
			return [32]byte{}, err
		}
	}

	// 更新记录数
	newPage.Header.RecordCount = p.Header.RecordCount - midIndex
	p.Header.RecordCount = midIndex

	// 返回中间键
	var middleKey [32]byte
	copy(middleKey[:], p.Key[midIndex*32:(midIndex+1)*32])

	return middleKey, nil
}

// 插入内部记录
func (p *Page) InsertInternalRecord(record *Record.InternalRecord) error {
	if p.Header.RecordCount >= p.Header.MaxRecordCount {
		return fmt.Errorf("页面已满")
	}

	// 1. 找到插入位置
	insertPos := 0
	recordKey := record.GetKey()

	// 二分查找插入位置
	left, right := 0, int(p.Header.RecordCount)-1
	for left <= right {
		mid := (left + right) / 2
		// 读取mid位置的key
		currentKey, err := p.ReadKey(uint32(mid*32), 32)
		if err != nil {
			return fmt.Errorf("读取key失败: %v", err)
		}

		cmp := bytes.Compare(recordKey[:], currentKey[:])
		if cmp == 0 {
			return fmt.Errorf("key已存在")
		} else if cmp < 0 {
			right = mid - 1
		} else {
			left = mid + 1
		}
	}
	insertPos = left

	// 2. 移动现有记录，为新记录腾出空间
	if insertPos < int(p.Header.RecordCount) {
		// 移动key
		for i := int(p.Header.RecordCount); i > insertPos; i-- {
			key, err := p.ReadKey(uint32((i-1)*32), 32)
			if err != nil {
				return fmt.Errorf("读取key失败: %v", err)
			}
			if err := p.WriteKey(uint32(i*32), key); err != nil {
				return fmt.Errorf("写入key失败: %v", err)
			}
		}

		// 移动内部记录数据
		for i := int(p.Header.RecordCount); i > insertPos; i-- {
			value, err := p.ReadValue(uint32((i-1)*int(Record.InternalRecordSize)), uint32(Record.InternalRecordSize))
			if err != nil {
				return fmt.Errorf("读取内部记录失败: %v", err)
			}
			if err := p.WriteValue(uint32(i*int(Record.InternalRecordSize)), value); err != nil {
				return fmt.Errorf("写入内部记录失败: %v", err)
			}
		}
	}

	// 3. 写入新记录
	if err := p.WriteKey(uint32(insertPos*32), recordKey[:]); err != nil {
		return fmt.Errorf("写入key失败: %v", err)
	}

	// 序列化内部记录
	recordValue, err := record.SerializeTo()

	if err != nil {
		return fmt.Errorf("序列化内部记录失败: %v", err)
	}

	// 写入内部记录数据
	if err := p.WriteValue(uint32(insertPos*int(Record.InternalRecordSize)), recordValue); err != nil {
		return fmt.Errorf("写入内部记录失败: %v", err)
	}

	// 4. 更新记录数
	p.Header.RecordCount++

	return nil
}

// 删除记录
func (p *Page) DeleteRecord(key [32]byte) error {
	// 二分查找记录位置
	left, right := 0, int(p.Header.RecordCount)-1
	for left <= right {
		mid := (left + right) / 2
		currentKey := p.Key[mid*32 : (mid+1)*32]
		cmp := bytes.Compare(key[:], currentKey)
		if cmp == 0 {
			// 找到记录，删除它
			return p.removeRecordAt(mid)
		} else if cmp < 0 {
			right = mid - 1
		} else {
			left = mid + 1
		}
	}
	return fmt.Errorf("记录不存在")
}

// 移除指定位置的记录
func (p *Page) removeRecordAt(index int) error {
	if index < 0 || index >= int(p.Header.RecordCount) {
		return fmt.Errorf("索引越界")
	}

	// 移动后续记录
	for i := index; i < int(p.Header.RecordCount)-1; i++ {
		copy(p.Key[i*32:(i+1)*32], p.Key[(i+1)*32:(i+2)*32])
		copy(p.Value[i*int(p.Header.RecordSize):(i+1)*int(p.Header.RecordSize)],
			p.Value[(i+1)*int(p.Header.RecordSize):(i+2)*int(p.Header.RecordSize)])
	}

	p.Header.RecordCount--
	return nil
}

// 获取所有记录
func (p *Page) GetAllRecords() ([]*Record.Record, error) {
	records := make([]*Record.Record, p.Header.RecordCount)
	for i := uint32(0); i < p.Header.RecordCount; i++ {
		record, err := p.GetRecordAt(i)
		if err != nil {
			return nil, err
		}
		records[i] = record
	}
	return records, nil
}

// 获取指定位置的记录
func (p *Page) GetRecordAt(index uint32) (*Record.Record, error) {
	if index >= p.Header.RecordCount {
		return nil, fmt.Errorf("索引越界")
	}

	var key [32]byte
	copy(key[:], p.Key[index*32:(index+1)*32])

	var value [128]byte
	copy(value[:], p.Value[index*p.Header.RecordSize:(index+1)*p.Header.RecordSize])

	return Record.NewRecord(Record.RecordHeader{}, key, value), nil
}

func (p *Page) FindRecord(key [32]byte) (*Record.Record, error) {
	// 在叶子节点中查找记录
	for i := uint32(0); i < p.Header.RecordCount; i++ {
		currentKey := p.Key[i*32 : (i+1)*32]
		if bytes.Compare(key[:], currentKey[:]) == 0 {
			return p.GetRecordAt(i)
		}
	}
	return nil, fmt.Errorf("记录不存在")
}

// 获取最大键值
func (p *Page) GetMaxKey() []byte {
	if p.Header.RecordCount == 0 {
		return nil
	}
	return p.Key[(p.Header.RecordCount-1)*32 : p.Header.RecordCount*32]
}

// 移除最后一条记录
func (p *Page) RemoveLastRecord() (*Record.Record, error) {
	if p.Header.RecordCount == 0 {
		return nil, fmt.Errorf("页面为空")
	}

	record, err := p.GetRecordAt(p.Header.RecordCount - 1)
	if err != nil {
		return nil, err
	}

	p.Header.RecordCount--
	return record, nil
}

// 移除第一条记录
func (p *Page) RemoveFirstRecord() (*Record.Record, error) {
	if p.Header.RecordCount == 0 {
		return nil, fmt.Errorf("页面为空")
	}

	record, err := p.GetRecordAt(0)
	if err != nil {
		return nil, err
	}

	// 移动其他记录
	for i := uint32(0); i < p.Header.RecordCount-1; i++ {
		copy(p.Key[i*32:(i+1)*32], p.Key[(i+1)*32:(i+2)*32])
		copy(p.Value[i*p.Header.RecordSize:(i+1)*p.Header.RecordSize],
			p.Value[(i+1)*p.Header.RecordSize:(i+2)*p.Header.RecordSize])
	}

	p.Header.RecordCount--
	return record, nil
}

// 移除最后一条内部记录
func (p *Page) RemoveLastInternalRecord() (*Record.InternalRecord, error) {
	if p.Header.RecordCount == 0 {
		return nil, fmt.Errorf("页面为空")
	}

	record := p.GetInternalRecord(int(p.Header.RecordCount - 1))
	p.Header.RecordCount--
	return record, nil
}

// 移除第一条内部记录
func (p *Page) RemoveFirstInternalRecord() (*Record.InternalRecord, error) {
	if p.Header.RecordCount == 0 {
		return nil, fmt.Errorf("页面为空")
	}

	record := p.GetInternalRecord(0)

	// 移动其他记录
	for i := 0; i < int(p.Header.RecordCount-1); i++ {
		copy(p.Key[i*32:(i+1)*32], p.Key[(i+1)*32:(i+2)*32])
		copy(p.Value[i*int(Record.InternalRecordSize):(i+1)*int(Record.InternalRecordSize)],
			p.Value[(i+1)*int(Record.InternalRecordSize):(i+2)*int(Record.InternalRecordSize)])
	}

	p.Header.RecordCount--
	return record, nil
}

// 获取第一条内部记录
func (p *Page) GetFirstInternalRecord() *Record.InternalRecord {
	if p.Header.RecordCount == 0 {
		return nil
	}
	return p.GetInternalRecord(0)
}

// 范围查询
func (p *Page) RangeQuery(startKey, endKey [32]byte) ([]*Record.Record, error) {
	var results []*Record.Record

	for i := uint32(0); i < p.Header.RecordCount; i++ {
		currentKey := p.Key[i*32 : (i+1)*32]
		if bytes.Compare(currentKey, startKey[:]) >= 0 && bytes.Compare(currentKey, endKey[:]) <= 0 {
			record, err := p.GetRecordAt(i)
			if err != nil {
				return nil, err
			}
			results = append(results, record)
		}
	}

	return results, nil
}

// 更新记录
func (p *Page) UpdateRecord(record *Record.Record) (error, *Record.Record) {
	// 找到记录
	for i := uint32(0); i < p.Header.RecordCount; i++ {
		currentKey := p.Key[i*32 : (i+1)*32]
		if bytes.Compare(record.Key[:], currentKey[:]) == 0 {
			oldRecord, err := p.GetRecordAt(i)
			if err != nil {
				return err, nil
			}
			return p.UpdateRecordAt(i, record), oldRecord
		}
	}
	return fmt.Errorf("记录不存在"), nil
}

func (p *Page) UpdateRecordAt(index uint32, record *Record.Record) error {
	if index >= p.Header.RecordCount {
		return fmt.Errorf("索引越界")
	}

	p.WriteKey(uint32(index*32), record.Key[:])
	p.WriteValue(uint32(index*p.Header.RecordSize), record.Value[:])
	return nil
}
