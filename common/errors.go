package common

import (
	"errors"
	"fmt"
)

type ErrNo struct {
	Code    int
	Message string
}

func (e *ErrNo) Error() string {
	return e.Message
}

type Err struct {
	Code    int    // 错误码
	Message string // 展示给用户看的错误信息
	Err     error  // 保存内部错误信息
}

func (err *Err) Error() string {
	return fmt.Sprintf("Err - code: %d, message: %s, error: %s", err.Code, err.Message, err.Err)
}

func NewErr(errno *ErrNo, err error) *Err {
	return &Err{
		Code:    errno.Code,
		Message: errno.Message,
		Err:     err,
	}
}

// ExtractErrCode 提取错误中的错误码/**
func ExtractErrCode(e error) int {
	var err *Err
	if errors.As(e, &err) {
		return err.Code
	}
	return InnerErrNo.Code
}

var (
	InnerErrNo           = ErrNo{Code: 10001, Message: "内部异常"}
	KeyIsEmptyErrNo      = ErrNo{Code: 10002, Message: "the key is empty"}
	KeyNotFoundErrNo     = ErrNo{Code: 10003, Message: "key not found in database"}
	DatabaseIsUsingErrNo = ErrNo{Code: 10004, Message: "the database directory is used by another process"}
	ReadOnlyBatchErrNo   = ErrNo{Code: 10005, Message: "the batch is read only"}
	BatchCommittedErrNo  = ErrNo{Code: 10006, Message: "the batch is committed"}
	DBClosedErrNo        = ErrNo{Code: 10007, Message: "the database is closed"}
	MergeRunningErrNo    = ErrNo{Code: 10008, Message: "the merge operation is running"}
	UnknownActionErrNo   = ErrNo{Code: 10009, Message: "未知行为无法处理"}
)

var (
	ErrKeyIsEmpty      = errors.New("the key is empty")
	ErrKeyNotFound     = errors.New("key not found in database")
	ErrDatabaseIsUsing = errors.New("the database directory is used by another process")
	ErrReadOnlyBatch   = errors.New("the batch is read only")
	ErrBatchCommitted  = errors.New("the batch is committed")
	ErrDBClosed        = errors.New("the database is closed")
	ErrMergeRunning    = errors.New("the merge operation is running")
	ErrUnknownAction   = errors.New("未知行为无法处理")
)
