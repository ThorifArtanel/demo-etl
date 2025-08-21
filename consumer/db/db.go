package db

import (
	"database/sql"

	_ "github.com/marcboeker/go-duckdb/v2"
)

func NewConn() (conn *sql.DB, err error) {
	conn, err = sql.Open("duckdb", "")
	if err != nil {
		return
	}
	_, err = conn.Exec(SetSecretS3())
	if err != nil {
		return
	}

	_, err = conn.Exec("SET s3_use_ssl = false; INSTALL excel; LOAD excel;")
	if err != nil {
		return
	}

	err = conn.Ping()
	if err != nil {
		return
	}

	return
}

func CloseConn(conn *sql.DB) (err error) {
	err = conn.Close()
	if err != nil {
		return
	}
	return
}

func SetSecretS3() string {
	return "CREATE SECRET my_secret ( TYPE S3, KEY_ID 'R3CPFN8PWLCYN06J6OJ7', SECRET '8KVcXpaNLuiiC7pVD2E7wevImSUj4iI1u5vaaC1l', URL_STYLE 'path', ENDPOINT 'host.docker.internal:9000')"
}
