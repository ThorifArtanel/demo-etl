package main

import (
	"consumer/db"

	"github.com/rs/zerolog/log"
)

func CSV3() (err error) {
	conn, err := db.NewConn()
	if err != nil {
		log.Fatal().Msgf("Failed to connect to database: %+v", err)
	}
	defer db.CloseConn(conn)

	conn.Exec(`COPY (
	SELECT
    l_orderkey as order_id,
    sum(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate as order_date
	FROM
			's3://demo-etl/customer.parquet',
			's3://demo-etl/orders.parquet',
			's3://demo-etl/lineitem.parquet'
	WHERE
			c_mktsegment = 'BUILDING'
			AND c_custkey = o_custkey
			AND l_orderkey = o_orderkey
			AND o_orderdate < CAST('1995-03-15' AS date)
			AND l_shipdate > CAST('1995-03-15' AS date)
	GROUP BY
			l_orderkey,
			o_orderdate,
			o_shippriority
	ORDER BY
			revenue DESC,
			o_orderdate
	) TO 's3://demo-etl-generated/csv/3.csv' (HEADER, DELIMITER ';');`)
	return
}

func XLSX3() (err error) {
	conn, err := db.NewConn()
	if err != nil {
		log.Fatal().Msgf("Failed to connect to database: %+v", err)
	}
	defer db.CloseConn(conn)

	conn.Exec(`COPY (
	SELECT
    l_orderkey as order_id,
    sum(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate as order_date
	FROM
			's3://demo-etl/customer.parquet',
			's3://demo-etl/orders.parquet',
			's3://demo-etl/lineitem.parquet'
	WHERE
			c_mktsegment = 'BUILDING'
			AND c_custkey = o_custkey
			AND l_orderkey = o_orderkey
			AND o_orderdate < CAST('1995-03-15' AS date)
			AND l_shipdate > CAST('1995-03-15' AS date)
	GROUP BY
			l_orderkey,
			o_orderdate,
			o_shippriority
	ORDER BY
			revenue DESC,
			o_orderdate
	) TO 's3://demo-etl-generated/xlsx/3.xlsx' WITH (FORMAT xlsx, HEADER true, SHEET 'Generated');`)
	return
}
