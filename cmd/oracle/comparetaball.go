package oracle

import (
	go_oracle "github.com/sijms/go-ora/v2"
	//log "github.com/sirupsen/logrus"
	"log"
	"oracle_infocoll/package/db"
)

type Col struct {
	colname string
	coltype string
}

type TableCol struct {
	owner     string
	tablename string
	//pk        string
	//partition bool
	//cols []Col
	colname       string
	coltype       string
	datalength    int
	dataprecision int
	datascale     int
	nullable      string
}

func GetSchemaColInfo(op *db.Options, schema string) ([]TableCol, error) {
	db, err := db.New(op)
	if err != nil {
		log.Printf("db new error: %s", err.Error())
		return nil, err
	}
	// close db
	defer func() {
		err := db.Close()
		if err != nil {
			log.Printf("db close error: %s", err.Error())
		}
	}()

	res, err := db.Query("select owner,table_name,column_name,data_type,data_length,nvl(data_precision,0),nvl(data_scale,0),nullable from dba_tab_cols where owner = 'SYSTEM'")
	if err != nil {
		log.Printf("db query err :%s", err)
		return nil, err
	}
	defer res.Close()
	var (
		onecol TableCol
		cols   []TableCol
	)
	for res.Next() {
		err := res.Scan(&onecol.owner, &onecol.tablename, &onecol.colname, &onecol.coltype, &onecol.datalength, &onecol.dataprecision, &onecol.datascale, &onecol.nullable)
		if err != nil {
			log.Printf("row scan err :%s", err)
			return nil, err
		}
		cols = append(cols, onecol)
	}
	return cols, nil

}

func GetSchemsColInfo(conn *go_oracle.Connection) ([]TableCol, error) {
	stmt := go_oracle.NewStmt("select owner,table_name,column_name,data_type,data_length,nvl(data_precision,0),nvl(data_scale,0),nullable from dba_tab_cols where owner = 'SYSTEM'", conn)
	//
	defer func() {
		stmt.Close()
	}()
	rows, err := stmt.Query_(nil)
	if err != nil {
		log.Printf("stmt query error: %s", err)
		return nil, err
	}
	var (
		onecol TableCol
		cols   []TableCol
	)
	for rows.Next_() {
		err = rows.Scan(&onecol.owner, &onecol.tablename, &onecol.colname, &onecol.coltype, &onecol.datalength, &onecol.dataprecision, &onecol.datascale, &onecol.nullable)
		if err != nil {
			log.Printf("rows Scan error: %s", err)
			return nil, err
		}
		cols = append(cols, onecol)
	}
	return cols, nil
}
