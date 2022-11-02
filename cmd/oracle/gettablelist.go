package oracle

import (
	"database/sql/driver"
	gooracle "github.com/sijms/go-ora/v2"
	log "github.com/sirupsen/logrus"
)

type tablist struct {
	serialNumber int
	dept         string
	systemName   string
	owner        string
	dbName       string
	tableNameE   string
	tableNameC   string
	tabComment   string
	lastUpdated  string
	numRows      int
	columnNums   int
}

func GetTableInfo(conn *gooracle.Connection, sql string, owner string, tableName string) tablist {
	stmt := gooracle.NewStmt(sql, conn)
	defer func() {
		stmt.Close()
	}()

	res, err := stmt.Query_([]driver.Value{owner, tableName})
	if err != nil {
		log.Error(err)
	}
	defer func() {
		res.Close()
	}()

	var tl tablist
	for res.Next_() {
		res.Scan(&tl.systemName,
			&tl.owner,
			&tl.dbName,
			&tl.tableNameE,
			&tl.tableNameC,
			&tl.tabComment,
			&tl.lastUpdated,
			&tl.numRows,
			&tl.columnNums)
	}
	return tl
}
