package db

import (
	"database/sql"
	"fmt"
	_ "github.com/sijms/go-ora/v2"
	"log"
)

type Options struct {
	HostIP   string
	Username string
	Password string
	Server   string
	Port     string
}

func New(opt *Options) (*sql.DB, error) {
	//oracle://system:test1126@192.168.21.160:1521/orcl
	connString := fmt.Sprintf("oracle://%s:%s@%s:%s/%s", opt.Username, opt.Password, opt.HostIP, opt.Port, opt.Server)
	db, err := sql.Open("oracle", connString)
	if err != nil {
		log.Printf("sql open error: %s\n", err)
		return nil, err
	}
	db.SetMaxOpenConns(2)
	return db, nil
}
