package main

import (
	go_oracle "github.com/sijms/go-ora/v2"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"io"
	"oracle_infocoll/cmd/oracle"
	"os"
)

func init() {
	filename := "generate_table_dictionary.log"
	logFile, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766)
	if err != nil {
		panic(err)
	}
	mw := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(mw)

}

func main() {
	log.Info("starting generate")
	log.Info("starting read get_tab_dict.toml")
	viper.SetConfigName("get_tab_dict")
	viper.SetConfigType("toml")
	viper.AddConfigPath(".")
	if err := viper.ReadInConfig(); err != nil {
		log.Error(err)
	}
	connStr := viper.GetString("database_info.conn_str")
	sql_tablist := viper.GetString("query_sql.sql_tablist")
	sql_tabdict := viper.GetString("query_sql.sql_tabdict")
	tabList := viper.GetStringSlice("table_list.tables")
	title_tablist := viper.GetStringSlice("title_list.title_tablist")
	title_tabdict := viper.GetStringSlice("title_list.title_tabdict")
	conn, err := go_oracle.NewConnection(connStr)
	if err != nil {
		log.Error(err)
		os.Exit(-1)
	}
	if err := conn.Open(); err != nil {
		log.Error(err)
		os.Exit(-1)
	}
	defer func() {
		conn.Close()
	}()
	oracle.GenerateExcelAll(conn, sql_tabdict, sql_tablist, tabList, title_tabdict, title_tablist)
	select {}
}

func GetSlowSQL() {
	// parse toml
	viper.SetConfigName("oracle_getslowsql")
	viper.SetConfigType("toml")
	viper.AddConfigPath(".")
	if err := viper.ReadInConfig(); err != nil {
		log.Error("read config file failed : %v", err)
	}

	conn_str := viper.GetString("database_info.conn_str")
	begin_snapid := viper.GetInt("selection_info.begin_snapid")
	end_snapid := viper.GetInt("selection_info.end_snapid")
	group := viper.GetString("selection_info.group")
	topN := viper.GetInt("selection_info.topN")
	conn, err := go_oracle.NewConnection(conn_str)

	if err != nil {
		log.Error("go oracle new connection err: ", err)
	}

	if err := conn.Open(); err != nil {
		log.Fatal(err)
		os.Exit(-1)
	}

	defer func() {
		conn.Close()
	}()

	sqls, err := oracle.Getsqlinfo(conn, begin_snapid, end_snapid, group, topN)
	if err != nil {
		log.Error("get sql info error: ", err)
	}

	snaps, err := oracle.Getsnapinfo(conn, begin_snapid, end_snapid)
	if err != nil {
		log.Error("get sql info error: ", err)
	}

	sqlwithplans := oracle.Splicesql(sqls, conn)
	oracle.Generatexcel(sqlwithplans, snaps)
}
