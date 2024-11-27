package Record

import (
	"bytes"
	"encoding/binary"
	"unsafe"
)

type InternalRecord struct {
	header       RecordHeader
	Key          [32]byte
	FrontPointer uint32
	NextPointer  uint32
	Reserved     [128]byte
}

const (
	InternalRecordSize = unsafe.Sizeof(InternalRecord{})
)

func NewInternalRecord(header RecordHeader, key [32]byte, frontPointer uint32, nextPointer uint32) *InternalRecord {
	return &InternalRecord{header: header, Key: key, FrontPointer: frontPointer, NextPointer: nextPointer}
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
	return ir.header
}

func (ir *InternalRecord) SetHeader(header RecordHeader) {
	ir.header = header
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
