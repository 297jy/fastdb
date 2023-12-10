package fastdb

import (
	"bytes"
	"encoding/json"
	"fastdb/fastdb/params"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"net/http"
	"testing"
)

func TestHTTP_Server_Run(t *testing.T) {
	testKey := "key"
	testVal := "val"

	r := doPut(testKey, testVal)
	assert.Equal(t, r.Status, true)
	r = doGet(testKey)
	assert.Equal(t, r.Data, testVal)
	r = doDelete(testKey)
	assert.Equal(t, r.Status, true)
	r = doGet(testKey)
	assert.Equal(t, r.Status, false)
}

func doGet(key string) params.FastDbReply {
	p := params.FastDbRequest{
		Key:    key,
		Action: params.GetAction,
	}
	pjson, _ := json.Marshal(p)
	reader := bytes.NewReader(pjson)
	request, _ := http.NewRequest("POST", "http://localhost:6666/single", reader)
	request.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	response, _ := client.Do(request)
	defer response.Body.Close()
	body, _ := ioutil.ReadAll(response.Body)
	fmt.Printf(string(body))

	r := params.FastDbReply{}
	_ = json.Unmarshal(body, &r)
	return r
}

func doPut(key string, val string) params.FastDbReply {
	p := params.FastDbRequest{
		Key:    key,
		Value:  val,
		Action: params.PutAction,
	}
	pjson, _ := json.Marshal(p)
	reader := bytes.NewReader(pjson)
	request, _ := http.NewRequest("POST", "http://localhost:6666/single", reader)
	request.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	response, _ := client.Do(request)
	defer response.Body.Close()
	body, _ := ioutil.ReadAll(response.Body)
	fmt.Printf("doPut:" + string(body))

	r := params.FastDbReply{}
	_ = json.Unmarshal(body, &r)
	return r
}

func doDelete(key string) params.FastDbReply {
	p := params.FastDbRequest{
		Key:    key,
		Action: params.DeleteAction,
	}
	pjson, _ := json.Marshal(p)
	reader := bytes.NewReader(pjson)
	request, _ := http.NewRequest("POST", "http://localhost:6666/single", reader)
	request.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	response, _ := client.Do(request)
	defer response.Body.Close()
	body, _ := ioutil.ReadAll(response.Body)
	fmt.Printf("doDelete:" + string(body))

	r := params.FastDbReply{}
	_ = json.Unmarshal(body, &r)
	return r

}
