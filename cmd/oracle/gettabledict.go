package oracle

import (
	"database/sql/driver"
	gooracle "github.com/sijms/go-ora/v2"
	log "github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type tabledict struct {
	colid      int
	tname      string
	tcomments  string
	cname      string
	coltype    string
	isnullball string
	colcomment string
	isparted   string
	keycols    string
}

func GetTabDict(conn *gooracle.Connection, sql string, owner string, tabname string) []tabledict {
	stmt := gooracle.NewStmt(sql, conn)
	defer func() {
		stmt.Close()
	}()

	res, err := stmt.Query_([]driver.Value{owner, tabname, owner, tabname})

	if err != nil {
		log.Error(err)
	}
	defer func() {
		res.Close()
	}()
	var tab tabledict
	var tabDicts []tabledict
	for res.Next_() {
		res.Scan(&tab.colid,
			&tab.tname,
			&tab.tcomments,
			&tab.cname,
			&tab.coltype,
			&tab.isnullball,
			&tab.colcomment,
			&tab.isparted,
			&tab.keycols)
		tabDicts = append(tabDicts, tab)
	}
	return tabDicts
}

func GenerateExcelDetail(conn *gooracle.Connection, sql string, tablelist []string, etitle []string) {
	f := excelize.NewFile()
	for _, tab := range tablelist {
		// set title style
		styleTitle, err := f.NewStyle(&excelize.Style{
			Fill: excelize.Fill{Type: "pattern", Color: []string{"#66B2FF"}, Pattern: 1},
			Font: &excelize.Font{
				Bold:   true,
				Family: "仿宋",
				Size:   10,
			},
			Alignment: &excelize.Alignment{
				Vertical: "center",
			},
		})
		if err != nil {
			log.Error(err)
		}
		// set data stype
		styleData, err := f.NewStyle(&excelize.Style{
			Font: &excelize.Font{
				Bold:   false,
				Family: "仿宋",
				Size:   9,
			},
			Alignment: &excelize.Alignment{
				Vertical: "center",
			},
		})
		if err != nil {
			log.Error(err)
		}

		ownTab := strings.Split(tab, ".")
		if len(ownTab) == 2 {
			tabdict := GetTabDict(conn, sql, ownTab[0], ownTab[1])
			// 0 means table not exists in database
			if len(tabdict) == 0 {
				log.Warn(tab + " not exist in database ... ")
			} else {
				f.NewSheet(strings.ToLower(tab))
				if err := f.SetColWidth(tab, "A", string('A'+int32(len(etitle))), 25); err != nil {
					log.Error(err)
				}
				// write title
				n := 0
				for i := 'A'; i < 'A'+int32(len(etitle)); i++ {
					f.SetCellValue(tab, string(i)+"1", etitle[n])
					f.SetCellStyle(tab, string(i)+"1", string(i)+"1", styleTitle)
					n++
				}
				// write data
				for index, v := range tabdict {
					for j := 'A'; j < 'A'+int32(len(etitle)); j++ {
						f.SetCellValue(tab, string(j)+strconv.Itoa(index+2), reflect.ValueOf(v).Field(int(j-'A')))
						f.SetCellStyle(tab, string(j)+strconv.Itoa(index+2), string(j)+strconv.Itoa(index+2), styleData)
					}
				}
				log.Info("table " + tab + " generated")
			}

		} else {
			log.Warn("toml table list config error with : " + tab)
		}

	}
	f.DeleteSheet("Sheet1")
	fileName := "output_Oracle_TabDict_" + time.Now().Format("20060102150405") + ".xlsx"
	if err := f.SaveAs(fileName); err != nil {
		log.Error(err)
	}
	log.Info("excel successfully generated with file " + fileName)
}

func GenerateExcelAll(conn *gooracle.Connection, sql_tabdict string, sql_tablist string, tablelist []string, title_tabdict []string, title_tablist []string) {
	f := excelize.NewFile()

	// set style
	styleTitle, err := f.NewStyle(&excelize.Style{
		Fill: excelize.Fill{Type: "pattern", Color: []string{"#9BC2E6"}, Pattern: 1},
		Font: &excelize.Font{
			Bold:   true,
			Family: "仿宋",
			Size:   10,
		},
		Alignment: &excelize.Alignment{
			Vertical: "center",
		},
	})
	if err != nil {
		log.Error(err)
	}
	styleData, err := f.NewStyle(&excelize.Style{
		Font: &excelize.Font{
			Bold:   false,
			Family: "仿宋",
			Size:   10,
		},
		Alignment: &excelize.Alignment{
			Vertical: "center",
		},
	})
	if err != nil {
		log.Error(err)
	}

	// 表1数据表清单
	sh1str := "表1 数据表清单"
	f.NewSheet(sh1str)
	if err := f.SetColWidth(sh1str, "A", string('A'+int32(len(title_tablist))), 20); err != nil {
		log.Error(err)
	}
	for index, value := range title_tablist {
		f.SetCellValue(sh1str, string('A'+int32(index))+"1", value)
		f.SetCellStyle(sh1str, string('A'+int32(index))+"1", string('A'+int32(index))+"1", styleTitle)
	}

	seq := 1
	for _, value := range tablelist {
		tab := strings.Split(value, ".")
		if len(tab) != 2 {
			log.Warn("table name confure error with :" + value)
		} else {
			tabList := GetTableInfo(conn, sql_tablist, tab[0], tab[1])
			if tabList.tableNameE == "" {
				log.Warn("table not exists in database: " + value)
			} else {
				tabList.serialNumber = seq
				for j := 'A'; j < 'A'+int32(len(title_tablist)); j++ {
					f.SetCellValue(sh1str, string(j)+strconv.Itoa(seq+1), reflect.ValueOf(tabList).Field(int(j-'A')))
					f.SetCellStyle(sh1str, string(j)+strconv.Itoa(seq+1), string(j)+strconv.Itoa(seq+1), styleData)
				}
				seq++
			}
		}
	}

	// 表2数据字段信息表
	sh2str := "表2 数据字段信息表"
	f.NewSheet(sh2str)
	if err := f.SetColWidth(sh2str, "A", string('A'+int32(len(title_tabdict))), 20); err != nil {
		log.Error(err)
	}
	for index, value := range title_tabdict {
		f.SetCellValue(sh2str, string('A'+int32(index))+"1", value)
		f.SetCellStyle(sh2str, string('A'+int32(index))+"1", string('A'+int32(index))+"1", styleTitle)
	}

	excelRow := 2
	for _, tab := range tablelist {
		ownTab := strings.Split(tab, ".")
		if len(ownTab) == 2 {
			tabdict := GetTabDict(conn, sql_tabdict, ownTab[0], ownTab[1])
			// 0 means table not exists in database
			if len(tabdict) == 0 {
				log.Warn(tab + " not exist in database ... ")
			} else {
				for index, v := range tabdict {
					for j := 'A'; j < 'A'+int32(len(title_tabdict)); j++ {
						f.SetCellValue(sh2str, string(j)+strconv.Itoa(excelRow), reflect.ValueOf(v).Field(int(j-'A')))
						f.SetCellStyle(sh2str, string(j)+strconv.Itoa(excelRow), string(j)+strconv.Itoa(index+2), styleData)
					}
					excelRow++
				}
				log.Info("table " + tab + " generated")
			}

		} else {
			log.Warn("toml table list config error with : " + tab)
		}

	}

	f.DeleteSheet("Sheet1")
	fileName := "Oracle_TableDictionary_output_" + time.Now().Format("20060102150405") + ".xlsx"
	if err := f.SaveAs(fileName); err != nil {
		log.Error(err)
	}
	log.Info("excel successfully generated with file " + fileName)

}
