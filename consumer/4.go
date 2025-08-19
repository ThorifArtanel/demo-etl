package main

import (
	"bufio"
	bucketfs "consumer/bucket-fs"
	"consumer/db"
	"fmt"
	"os"

	"github.com/rs/zerolog/log"
)

func TXT4() (err error) {
	conn, err := db.NewConn()
	if err != nil {
		log.Fatal().Msgf("Failed to connect to database: %+v", err)
	}
	defer db.CloseConn(conn)

	rows, err := conn.Query(`
	SELECT
			o_orderpriority,
			count(*) AS order_count
	FROM
			's3://demo-etl/orders.parquet',
	WHERE
			o_orderdate >= CAST('1993-07-01' AS date)
			AND o_orderdate < CAST('1993-10-01' AS date)
			AND EXISTS (
					SELECT
							*
					FROM
							's3://demo-etl/lineitem.parquet'
					WHERE
							l_orderkey = o_orderkey
							AND l_commitdate < l_receiptdate)
	GROUP BY
			o_orderpriority
	ORDER BY
			o_orderpriority;`)
	if err != nil {
		return
	}
	defer rows.Close()

	fname := "/tmp/4.txt"
	f, err := os.Create(fname)
	if err != nil {
		return
	}

	defer f.Close()
	w := bufio.NewWriter(f)
	_, err = w.WriteString("order-priority --- order-count\n")
	if err != nil {
		return
	}
	for rows.Next() {
		var orderPrio string
		var count int
		err = rows.Scan(&orderPrio, &count)
		if err != nil {
			return
		}
		_, err = fmt.Fprintf(w, "%s --- %d\n", orderPrio, count)
		if err != nil {
			return
		}
	}
	w.Flush()

	err = bucketfs.StoreToBucket("txt/4.txt", fname)
	if err != nil {
		return
	}
	return
}
