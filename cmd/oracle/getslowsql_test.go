package oracle

import (
	go_oracle "github.com/sijms/go-ora/v2"
	"testing"
)

func TestGetsqlinfo(t *testing.T) {
	//cols, err := GetSchemaColInfo(op, "system")
	conn, err := go_oracle.NewConnection("oracle://system:test1126@192.168.21.160:1521/orcl")
	err = conn.Open()
	defer func() {
		conn.Close()
	}()
	if err != nil {
		t.Fatal("new connection err:", "error:", err)
	}
	sqls, err := Getsqlinfo(conn, 192, 193, "cpu_time", 20)

	sqlwithplans := Splicesql(sqls, conn)
	snaps, err := Getsnapinfo(conn, 192, 193)
	Generatexcel(sqlwithplans, snaps)
	if err != nil {
		t.Fatal("getsqlinfor:", "error:", err)
	} else {
		t.Log("result:", "execute finish")
	}
}
