package Transaction

import (
	"sync"
	"wudb/Util"
)

type LogManager struct {
	transactionLogs map[int32]*TransactionLog
	mutex           sync.Mutex
	fileHandle      *Util.FileHandle
}

func NewLogManager(logFileName string) *LogManager {
	fileHandle, err := Util.NewFileHandleWithCreate(logFileName)
	if err != nil {
		panic(err)
	}
	return &LogManager{
		transactionLogs: make(map[int32]*TransactionLog),
		mutex:           sync.Mutex{},
		fileHandle:      fileHandle,
	}

}

func (lm *LogManager) AddLog(log *TransactionLog) {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()
	lm.transactionLogs[log.TransactionID] = log
}

func (lm *LogManager) WriteTransactionLog(log *TransactionLog) {
	lm.fileHandle.Write([]byte(log.Output()))
	lm.fileHandle.Write([]byte("\n"))
	for _, operation := range log.Operations {
		lm.fileHandle.Write([]byte(operation.Output()))
		lm.fileHandle.Write([]byte("\n"))
	}
}

func (lm *LogManager) Undo(log *TransactionLog) {

}

func (lm *LogManager) Rollback(log *TransactionLog) {

}
