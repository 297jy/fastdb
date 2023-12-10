package params

const (
	GetAction    = "get"
	PutAction    = "put"
	DeleteAction = "delete"
)

type FastDbRequest struct {
	Key    string `json:"key"`
	Value  string `json:"value"`
	Action string `json:"action"`
}
