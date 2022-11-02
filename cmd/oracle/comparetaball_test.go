package oracle

import (
	go_oracle "github.com/sijms/go-ora/v2"
	"oracle_infocoll/package/db"
	"testing"
)

var op = &db.Options{
	HostIP:   "192.168.21.160",
	Username: "system",
	Password: "test1126",
	Server:   "orcl",
	Port:     "1521",
}

func TestAdd(t *testing.T) {
	//cols, err := GetSchemaColInfo(op, "system")
	conn, err := go_oracle.NewConnection("oracle://system:test1126@192.168.21.160:1521/orcl")
	err = conn.Open()

	defer func() {
		conn.Close()
	}()
	if err != nil {
		t.Fatal("new connection err:", op.HostIP, "error:", err)
	}
	//cols, err := GetSchemsColInfo(conn)
	//if err != nil {
	//	t.Fatal("get col info err:", op.HostIP, "error:", err)
	//} else {
	//	t.Log("result:", cols)
	//}
}
