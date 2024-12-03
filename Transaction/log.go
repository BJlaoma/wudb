package Transaction

import (
	"fmt"
	"time"
)

type TransactionLog struct {
	TransactionID    int32
	TransactionLevel int32
	BeginTime        time.Time
	EndTime          time.Time
	Status           uint8
	Operations       []Operation
}

func NewTransactionLog(transactionID int32, transactionLevel int32, beginTime time.Time) *TransactionLog {
	return &TransactionLog{
		TransactionID:    transactionID,
		TransactionLevel: transactionLevel,
		BeginTime:        beginTime,
		Status:           Active,
	}
}

func (tl *TransactionLog) AddOperation(operation Operation) {
	tl.Operations = append(tl.Operations, operation)
}

func (tl *TransactionLog) SetEndTime(endTime time.Time) {
	tl.EndTime = endTime
}

func (tl *TransactionLog) SetStatus(status uint8) {
	tl.Status = status
}

func (tl *TransactionLog) Output() string {
	return fmt.Sprintf("[%d] TransactionID: %d, TransactionLevel: %d, BeginTime: %s, EndTime: %s, Status: %d", time.Now().Unix(), tl.TransactionID, tl.TransactionLevel, tl.BeginTime, tl.EndTime, tl.Status)
}
