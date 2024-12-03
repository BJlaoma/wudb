package Transaction

import (
	"fmt"
	"time"
	"wudb/Entity/Record"
)

type Transaction struct {
	TransactionID     int32
	NextTransactionID int32
	IsolationLevel    int32
	BeginTime         time.Time
	EndTime           time.Time
	Status            uint8
	Operations        []Operation
	TransactionLog    *TransactionLog
}

type Operation struct {
	TransactionID int32
	OperationType int32
	PageID        int32
	Record        *Record.Record
	OldRecord     *Record.Record
}

const (
	InsertOperation = 0
	DeleteOperation = 1
	UpdateOperation = 2
	Active          = 0
	Committed       = 1
	Aborted         = 2
	ReadUncommitted = 0
	ReadCommitted   = 1
	RepeatableRead  = 2
	Serializable    = 3
)

func NewTransaction(transactionID int32, nextTransactionID int32, isolationLevel int32) *Transaction {
	return &Transaction{
		TransactionID:     transactionID,
		NextTransactionID: nextTransactionID,
		IsolationLevel:    isolationLevel,
		BeginTime:         time.Now(),
		Status:            Active,
		Operations:        make([]Operation, 0),
		TransactionLog:    NewTransactionLog(transactionID, isolationLevel, time.Now()),
	}
}

func (t *Transaction) AddOperation(operation Operation) {
	t.Operations = append(t.Operations, operation)
	t.TransactionLog.AddOperation(operation)
}

func (t *Transaction) SetEndTime(endTime time.Time) {
	t.EndTime = endTime
}

func (t *Transaction) SetStatus(status uint8) {
	t.Status = status
}

func (op *Operation) Output() string {
	return fmt.Sprintf("[%d] TransactionID: %d, OperationType: %d, PageID: %d, Record: %v, OldRecord: %v", time.Now().Unix(), op.TransactionID, op.OperationType, op.PageID, op.Record, op.OldRecord)
}
