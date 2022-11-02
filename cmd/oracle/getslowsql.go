package oracle

import (
	gooracle "github.com/sijms/go-ora/v2"
	log "github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
	"reflect"
	"strconv"
	"strings"
)

type sqlinfo struct {
	sqlid         string
	planhashvalue string
	sqltext       string
	parsingschema string
	elapsedtime   int64
	cputime       int64
	executions    int64
	sqlplan       string
}

type snapinfo struct {
	instancename string
	snapid       int
	end_time     string
}

func Getsnapinfo(conn *gooracle.Connection, begin_snapid int, end_snapid int) (*[]snapinfo, error) {
	sqlstr := "select b.instance_name,snap_id,to_char(end_interval_time,'yyyymmdd_hh24miss') " +
		"from dba_hist_snapshot a, v$instance b " +
		"where a.instance_number= b.instance_number " +
		"and (snap_id = " + strconv.Itoa(begin_snapid) + " or snap_id = " + strconv.Itoa(end_snapid) + ") " +
		"order by a.snap_id desc "
	stmt := gooracle.NewStmt(sqlstr, conn)
	defer func() {
		stmt.Close()
	}()

	rows, err := stmt.Query_(nil)
	if err != nil {
		log.Error("stmt query error: %s", err)
		return nil, err
	}
	var snap snapinfo
	var snaps []snapinfo
	for rows.Next_() {
		rows.Scan(&snap.instancename, &snap.snapid, &snap.end_time)
		snaps = append(snaps, snap)
	}
	return &snaps, nil
}

func Getsqlinfo(conn *gooracle.Connection, begin_snapid int, end_snapid int, group string, topN int) (*[]sqlinfo, error) {
	sqlstring := "SELECT sql_id,plan_hash_value,sql_text,parsing_schema_name,elapsed_time,cpu_time,executions " +
		" FROM table(DBMS_SQLTUNE.SELECT_WORKLOAD_REPOSITORY(" + strconv.Itoa(begin_snapid) + "," +
		strconv.Itoa(end_snapid) + ",null, null,  '" + group + "',null, null, null, " + strconv.Itoa(topN) + ")) "

	stmt := gooracle.NewStmt(sqlstring, conn)
	defer func() {
		stmt.Close()
	}()
	rows, err := stmt.Query_(nil)
	if err != nil {
		log.Error("stmt query error: %s", err)
		return nil, err
	}
	var sql sqlinfo
	sqls := make([]sqlinfo, 0, topN)
	for rows.Next_() {
		rows.Scan(&sql.sqlid,
			&sql.planhashvalue,
			&sql.sqltext,
			&sql.parsingschema,
			&sql.elapsedtime,
			&sql.cputime,
			&sql.executions)
		sqls = append(sqls, sql)
	}
	return &sqls, nil
}

func Getsqlplan(conn *gooracle.Connection, sqlid string) (string, error) {
	sqlstring := "select * from table(dbms_xplan.display_awr('" + sqlid + "'))"
	stmt := gooracle.NewStmt(sqlstring, conn)
	defer func() {
		stmt.Close()
	}()
	rows, err := stmt.Query_(nil)
	if err != nil {
		log.Error("stmt query error: %s", err)
		return "", err
	}
	var (
		plan  string
		plans string
	)
	//var plans string
	for rows.Next_() {
		rows.Scan(&plan)
		plans = plans + plan + "\n"
	}
	return plans, nil
}

func Splicesql(sql *[]sqlinfo, conn *gooracle.Connection) *[]sqlinfo {
	var sqls []sqlinfo
	for _, value := range *sql {
		plan, err := Getsqlplan(conn, value.sqlid)
		if err != nil {
			log.Error("get sql plan err: %s", err)
		}
		value.sqlplan = plan
		sqls = append(sqls, value)
	}
	return &sqls
}

func Generatexcel(sql *[]sqlinfo, snap *[]snapinfo) {
	f := excelize.NewFile()
	sheetname := "Top SQL"
	f.SetSheetName("Sheet1", sheetname)

	styleTitle, err := f.NewStyle(&excelize.Style{
		Fill: excelize.Fill{Type: "pattern", Color: []string{"#778899"}, Pattern: 1},
		Font: &excelize.Font{
			Bold: true,
			Size: 11,
		},
	})
	if err != nil {
		log.Error("excelize.Style error: %s", err)
	}

	// set title
	ts := sqlinfo{}
	t := reflect.TypeOf(ts)
	v := reflect.ValueOf(ts)
	//set width
	if err := f.SetColWidth(sheetname, "A", string('A'+int32(v.NumField())), 20); err != nil {
		log.Error("excel set col width err: %s", err)
	}
	for j := 'A'; j < 'A'+int32(v.NumField()); j++ {
		f.SetCellValue(sheetname, string(j)+"1", strings.ToUpper(t.Field(int(j-'A')).Name))
		f.SetCellStyle(sheetname, string(j)+"1", string(j)+"1", styleTitle)
	}

	// write sql info
	for index, sf := range *sql {
		for j := 'A'; j < 'A'+int32(v.NumField()); j++ {
			f.SetCellValue(sheetname, string(j)+strconv.Itoa(index+2), reflect.ValueOf(sf).Field(int(j-'A')))
		}
	}

	// splice excel name
	var (
		instname string
		snapids  string
		timedesc string
	)
	for _, value := range *snap {
		instname = value.instancename
		snapids = snapids + "_" + strconv.Itoa(value.snapid)
		timedesc = timedesc + "_" + value.end_time
	}
	if err := f.SaveAs(instname + snapids + timedesc + ".xlsx"); err != nil {
		log.Println(err)
	}
}
