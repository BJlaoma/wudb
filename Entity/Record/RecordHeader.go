package Record

import "time"

// 大小：32Byte
type RecordHeader struct {
	IsDeleted     uint8
	RecordLength  uint32
	KeySize       uint32
	ValueSize     uint32
	TransactionID uint32
	Timestamp     uint32
	Reserved      [11]byte
}

func NewRecordHeader() *RecordHeader {
	return &RecordHeader{
		IsDeleted:     0,
		RecordLength:  192,
		KeySize:       32,
		ValueSize:     128,
		TransactionID: 0,
		Timestamp:     uint32(time.Now().Unix()),
	}
}

// Getter 方法
func (rh *RecordHeader) GetIsDeleted() uint8 {
	return rh.IsDeleted
}

func (rh *RecordHeader) GetRecordLength() uint32 {
	return rh.RecordLength
}

func (rh *RecordHeader) GetKeySize() uint32 {
	return rh.KeySize
}

func (rh *RecordHeader) GetValueSize() uint32 {
	return rh.ValueSize
}

func (rh *RecordHeader) GetTransactionID() uint32 {
	return rh.TransactionID
}

func (rh *RecordHeader) GetTimestamp() uint32 {
	return rh.Timestamp
}

func (rh *RecordHeader) GetReserved() [11]byte {
	return rh.Reserved
}

// Setter 方法
func (rh *RecordHeader) SetIsDeleted(isDeleted uint8) {
	rh.IsDeleted = isDeleted
}

func (rh *RecordHeader) SetRecordLength(length uint32) {
	rh.RecordLength = length
}

func (rh *RecordHeader) SetKeySize(size uint32) {
	rh.KeySize = size
}

func (rh *RecordHeader) SetValueSize(size uint32) {
	rh.ValueSize = size
}

func (rh *RecordHeader) SetTransactionID(id uint32) {
	rh.TransactionID = id
}

func (rh *RecordHeader) SetTimestamp(timestamp uint32) {
	rh.Timestamp = timestamp
}
