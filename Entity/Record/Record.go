package Record

import (
	"bytes"
	"encoding/binary"
	"time"
)

type Record struct {
	Header RecordHeader
	Key    [32]byte
	Value  []byte
}

func NewRecord(header RecordHeader, key [32]byte, value []byte) *Record {
	return &Record{Header: header, Key: key, Value: value}
}

func NewRecordByTransaction(transactionID uint32, key [32]byte, value []byte) *Record {
	return &Record{
		Header: RecordHeader{
			TransactionID: transactionID,
			IsDeleted:     0,
			RecordLength:  uint32(len(key) + len(value) + 32),
			KeySize:       uint32(len(key)),
			ValueSize:     uint32(len(value)),
			Timestamp:     uint32(time.Now().Unix()),
		},
		Key:   key,
		Value: value,
	}
}

func (r *Record) GetKey() [32]byte {
	return r.Key
}

func (r *Record) GetValue() []byte {
	return r.Value
}

func (r *Record) SetValue(value []byte) {
	r.Value = value
}

func (r *Record) SetKey(key [32]byte) {
	r.Key = key
}

func (r *Record) GetRecordSize() uint32 {
	return r.Header.RecordLength
}

func (r *Record) SerializeTo() ([]byte, error) {
	buffer := bytes.NewBuffer(nil)
	binary.Write(buffer, binary.BigEndian, r.Header)
	buffer.Write(r.Key[:])
	buffer.Write(r.Value)
	return buffer.Bytes(), nil
}

func (r *Record) DeserializeFrom(data []byte) error {
	buffer := bytes.NewBuffer(data)
	return binary.Read(buffer, binary.BigEndian, r)
}
