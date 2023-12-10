package fastdb

import (
	"encoding/json"
	"fastdb/common"
	"fastdb/config"
	"fastdb/core"
	"fastdb/fastdb/params"
	"fastdb/interface/server"
	"fmt"
	"log"
	"net/http"
)

type httpServer struct {
	addr     string
	basePath string
	db       *core.DB
	options  config.ServerOptions
}

func MakeServer(options config.ServerOptions) (server.Server, error) {
	return &httpServer{
		options: options,
	}, nil
}

func (s *httpServer) handleSingleRequest(writer http.ResponseWriter, request *http.Request) {
	r, e := decodeRequest(request)
	if e != nil {
		encodeReply(writer, params.MakeErrReply(e))
		return
	}
	batch := s.db.NewBatch(s.options.BatchOptions)
	defer batch.Close()

	var val []byte
	switch r.Action {
	case params.GetAction:
		val, e = batch.Get([]byte(r.Key))
	case params.PutAction:
		e = batch.Put([]byte(r.Key), []byte(r.Value))
	case params.DeleteAction:
		e = batch.Delete([]byte(r.Key))
	default:
		e = common.NewErr(&common.UnknownActionErrNo, common.ErrUnknownAction)
	}
	if e != nil {
		encodeReply(writer, params.MakeErrReply(e))
		return
	}

	e = batch.Commit()
	if e != nil {
		encodeReply(writer, params.MakeErrReply(e))
		return
	}
	encodeReply(writer, params.MakeSuccessReply(val))
}

func decodeRequest(request *http.Request) (params.FastDbRequest, error) {
	var r params.FastDbRequest
	decoder := json.NewDecoder(request.Body)
	e := decoder.Decode(&r)
	if e != nil {
		return params.FastDbRequest{}, e
	}
	return r, nil
}

func encodeReply(writer http.ResponseWriter, reply params.FastDbReply) {
	e := json.NewEncoder(writer).Encode(reply)
	if e != nil {
		print(e)
	}
}

func (s *httpServer) Close() {
	print("正在关闭连接")
	err := s.db.Close()
	if err != nil {
		print(err)
	}
}

func (s *httpServer) Run() error {
	db, err := core.Open(s.options.DbOptions)
	if err != nil {
		return err
	}
	s.db = db

	http.HandleFunc("/single", s.handleSingleRequest)

	addr := fmt.Sprintf(":%d", s.options.Port)
	fmt.Println("Running at " + addr)

	err = http.ListenAndServe(addr, nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err.Error())
	}

	return nil
}
