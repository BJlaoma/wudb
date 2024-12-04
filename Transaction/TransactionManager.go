package Transaction

import (
	"fmt"
	"sync"
	"wudb/Util"
)

type TransactionManager struct {
	TransactionMap    map[int32]*Transaction
	mutex             sync.Mutex
	nextTransactionID int32
	logManager        *LogManager
}

func NewTransactionManager(logManager *LogManager) *TransactionManager {
	return &TransactionManager{
		TransactionMap: make(map[int32]*Transaction),
		mutex:          sync.Mutex{},
		logManager:     logManager,
	}
}
func NewTransactionManagerWithHandle(fileHandle *Util.FileHandle) *TransactionManager {
	return &TransactionManager{
		TransactionMap: make(map[int32]*Transaction),
		mutex:          sync.Mutex{},
		logManager:     NewLogManager(fileHandle.FileID + ".log"),
	}
}

func (tm *TransactionManager) AddTransaction(transaction *Transaction) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()
	if _, ok := tm.TransactionMap[transaction.TransactionID]; !ok {
		tm.TransactionMap[transaction.TransactionID] = transaction
	}
}

func (tm *TransactionManager) AddOperation(operation Operation) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()
	transaction, ok := tm.TransactionMap[operation.TransactionID]
	if !ok {
		transaction = NewTransaction(operation.TransactionID, tm.nextTransactionID, ReadCommitted) //默认是读未提交
		tm.TransactionMap[operation.TransactionID] = transaction
	}
	transaction.AddOperation(operation)
}

func (tm *TransactionManager) Commit(transactionID int32) error {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()
	transaction, ok := tm.TransactionMap[transactionID]
	if !ok {
		return fmt.Errorf("事务不存在")
	}
	transaction.Status = Committed
	tm.logManager.WriteTransactionLog(transaction.TransactionLog)
	return nil
}

func (tm *TransactionManager) Rollback(transactionID int32) error {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()
	transaction, ok := tm.TransactionMap[transactionID]
	if !ok {
		return fmt.Errorf("事务不存在")
	}
	transaction.Status = Aborted
	return nil
}

func (tm *TransactionManager) Undo(transactionID int32) error {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()
	transaction, ok := tm.TransactionMap[transactionID]
	if !ok {
		return fmt.Errorf("事务不存在")
	}
	tm.logManager.Undo(transaction.TransactionLog)

	// 如果操作列表为空，直接返回
	if len(transaction.Operations) == 0 {
		return nil
	}
	// 删除最后一个操作
	newOperations := make([]Operation, len(transaction.Operations)-1)
	copy(newOperations, transaction.Operations[:len(transaction.Operations)-1])
	transaction.Operations = newOperations

	return nil
}
