package params

import "fastdb/common"

type FastDbReply struct {
	Status bool   `json:"status"`
	Code   int    `json:"code"`
	Msg    string `json:"msg"`
	Data   string `json:"data"`
}

func MakeErrReply(err error) FastDbReply {
	return FastDbReply{
		Status: false,
		Code:   common.ExtractErrCode(err),
		Msg:    err.Error(),
	}
}

func MakeSuccessReply(val []byte) FastDbReply {
	return FastDbReply{
		Status: true,
		Data:   string(val),
	}
}
