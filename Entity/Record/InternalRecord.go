package Record

import (
	"bytes"
	"encoding/binary"
)

type InternalRecord struct {
	Header       RecordHeader
	Key          [32]byte
	FrontPointer uint32 // 前驱指针 4
	NextPointer  uint32 // 后继指针 4
	Reserved     [120]byte
}

const (
	InternalRecordSize = 192
)

func NewInternalRecord(header RecordHeader, key [32]byte, frontPointer uint32, nextPointer uint32) *InternalRecord {
	return &InternalRecord{Header: header, Key: key, FrontPointer: frontPointer, NextPointer: nextPointer}
}

func (ir *InternalRecord) GetKey() [32]byte {
	return ir.Key
}

func (ir *InternalRecord) GetFrontPointer() uint32 {
	return ir.FrontPointer
}

func (ir *InternalRecord) GetNextPointer() uint32 {
	return ir.NextPointer
}

func (ir *InternalRecord) GetHeader() RecordHeader {
	return ir.Header
}

func (ir *InternalRecord) SetHeader(header RecordHeader) {
	ir.Header = header
}

func (ir *InternalRecord) SetKey(key [32]byte) {
	ir.Key = key
}

func (ir *InternalRecord) SetFrontPointer(frontPointer uint32) {
	ir.FrontPointer = frontPointer
}

func (ir *InternalRecord) SetNextPointer(nextPointer uint32) {
	ir.NextPointer = nextPointer
}

func (ir *InternalRecord) SerializeTo() ([]byte, error) {
	buffer := make([]byte, InternalRecordSize)
	buf := bytes.NewBuffer(buffer[:0])
	binary.Write(buf, binary.LittleEndian, ir)
	return buffer, nil
}

func (ir *InternalRecord) DeserializeFrom(data []byte) error {
	buf := bytes.NewBuffer(data)
	return binary.Read(buf, binary.LittleEndian, ir)
}
