package config

import (
	"database/sql"
	"time"
)

var db *sql.DB = nil

func Connect() (*sql.DB, error) {
	if db == nil {
		conn, err := sql.Open("mysql", Configuration.MySQL().FormatDSN())
		if err != nil {
			return nil, err
		}
		if err := conn.Ping(); err != nil {
			return nil, err
		}
		conn.SetConnMaxLifetime(time.Second * 30)
		db = conn
	}
	return db, nil
}
