package main

import (
	bucketfs "consumer/bucket-fs"
	"consumer/db"
	"time"

	"github.com/marcboeker/go-duckdb/v2"
	"github.com/rs/zerolog/log"
	"github.com/signintech/gopdf"
)

var spacing float64 = 7.5

type Content struct {
	OrderId   string
	OrderDate string
	Revenue   string
}

func CSV3() (err error) {
	conn, err := db.NewConn()
	if err != nil {
		log.Fatal().Msgf("Failed to connect to database: %+v", err)
	}
	defer db.CloseConn(conn)

	_, err = conn.Exec(`COPY (
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
	GROUP BY
			l_orderkey,
			o_orderdate,
			o_shippriority
	ORDER BY
			revenue DESC,
			o_orderdate
	) TO 's3://demo-etl-generated/csv/3.csv' (HEADER, DELIMITER ';');`)
	if err != nil {
		return
	}
	return
}

func XLSX3() (err error) {
	conn, err := db.NewConn()
	if err != nil {
		log.Fatal().Msgf("Failed to connect to database: %+v", err)
	}
	defer db.CloseConn(conn)

	_, err = conn.Exec(`COPY (
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
	GROUP BY
			l_orderkey,
			o_orderdate,
			o_shippriority
	ORDER BY
			revenue DESC,
			o_orderdate
	) TO 's3://demo-etl-generated/xlsx/3.xlsx' WITH (FORMAT xlsx, HEADER true, SHEET 'Generated');`)
	if err != nil {
		return
	}
	return
}

func PDF3() (err error) {
	conn, err := db.NewConn()
	if err != nil {
		return
	}
	defer db.CloseConn(conn)

	pdf := &gopdf.GoPdf{}
	// Start the PDF with a custom page size (we'll adjust it later)
	pdf.Start(gopdf.Config{PageSize: *gopdf.PageSizeA4})
	pdf.AddTTFFont("font1", "./FreeSans.ttf")
	pdf.AddFooter(func() {
		pdf.SetFont("font1", "", 10)
		pdf.SetTextColor(120, 120, 120)
		pdf.SetXY(15, 818)
		pdf.Cell(nil, "lorem ipsum dolor sit amet")
	})

	pdf.SetFont("font1", "", 11)

	rows, err := conn.Query(`
	SELECT
    l_orderkey as order_id,
    o_orderdate as order_date,
    sum(l_extendedprice * (1 - l_discount)) AS revenue
	FROM
			's3://demo-etl/customer.parquet',
			's3://demo-etl/orders.parquet',
			's3://demo-etl/lineitem.parquet'
	WHERE
			c_mktsegment = 'BUILDING'
			AND c_custkey = o_custkey
			AND l_orderkey = o_orderkey
	GROUP BY
			l_orderkey,
			o_orderdate,
			o_shippriority
	ORDER BY
			revenue DESC,
			o_orderdate;`)
	if err != nil {
		return
	}
	defer rows.Close()

	iter := 0
	var content []Content
	fp := true
	for rows.Next() {
		var data Content
		var price duckdb.Decimal
		var date time.Time
		err = rows.Scan(&data.OrderId, &date, &price)
		if err != nil {
			return
		}
		data.Revenue = price.String()
		data.OrderDate = date.Format("2006-01-02")
		content = append(content, data)
		iter++

		if fp && iter == 26 {
			err = FirstPage(pdf, content)
			if err != nil {
				return
			}
			content = []Content{}
			iter = 0
			fp = false
		} else if iter == 30 {
			err = PageContent(pdf, content)
			if err != nil {
				return
			}
			content = []Content{}
			iter = 0
		}
	}

	// Save the PDF to the specified path
	fname := "report.pdf"
	pdf.WritePdf(fname)
	err = bucketfs.StoreToBucket("pdf/3.pdf", fname)
	if err != nil {
		return
	}
	return
}

func FirstPage(pdf *gopdf.GoPdf, content []Content) (err error) {
	var posY, posX, height float64
	var text string

	pdf.AddPage()
	pdf.SetFont("font1", "", 24)
	pdf.SetTextColor(0, 0, 0)
	posX = 15
	posY = 15

	pdf.SetXY(posX, posY)
	text = "[REPORT NAME]"
	pdf.Cell(nil, text)

	for _, v := range []string{"[Lorem ipsum dolor sit amet]", "[Lorem ipsum dolor sit amet]", "[Lorem ipsum dolor sit amet]"} {
		height, _ = pdf.MeasureCellHeightByText(text)
		pdf.SetFont("font1", "", 12)
		posY = posY + height + spacing
		pdf.SetXY(posX, posY)
		pdf.Cell(nil, v)
	}

	marginLeft := 15.0
	table := pdf.NewTableLayout(marginLeft, posY+30, 25, 5)
	// Set the style for table cells
	table.SetTableStyle(gopdf.CellStyle{
		FillColor: gopdf.RGBColor{R: 255, G: 255, B: 255},
		TextColor: gopdf.RGBColor{R: 0, G: 0, B: 0},
		FontSize:  10,
	})

	// Set the style for table header
	table.SetHeaderStyle(gopdf.CellStyle{
		BorderStyle: gopdf.BorderStyle{
			Bottom: true,
			Width:  2.0,
		},
		Font:     "font1",
		FontSize: 12,
	})

	table.SetCellStyle(gopdf.CellStyle{
		BorderStyle: gopdf.BorderStyle{
			Top:      true,
			Width:    0.5,
			RGBColor: gopdf.RGBColor{R: 0, G: 0, B: 0},
		},
		FillColor: gopdf.RGBColor{R: 255, G: 255, B: 255},
		TextColor: gopdf.RGBColor{R: 0, G: 0, B: 0},
		Font:      "font1",
		FontSize:  10,
	})

	table.AddColumn("Order Id", 188, "center")
	table.AddColumn("Order Date", 188, "center")
	table.AddColumn("Revenue", 188, "center")

	for _, v := range content {
		table.AddRow([]string{v.OrderId, v.OrderDate, v.Revenue})
	}

	// Draw the table
	table.DrawTable()
	return
}
func PageContent(pdf *gopdf.GoPdf, content []Content) (err error) {
	pdf.AddPage()
	tableStartY := 20.0
	marginLeft := 15.0

	table := pdf.NewTableLayout(marginLeft, tableStartY, 25, 5)
	// Set the style for table cells
	table.SetTableStyle(gopdf.CellStyle{
		FillColor: gopdf.RGBColor{R: 255, G: 255, B: 255},
		TextColor: gopdf.RGBColor{R: 0, G: 0, B: 0},
		FontSize:  10,
	})

	// Set the style for table header
	table.SetHeaderStyle(gopdf.CellStyle{
		BorderStyle: gopdf.BorderStyle{
			Bottom: true,
			Width:  2.0,
		},
		Font:     "font1",
		FontSize: 12,
	})

	table.SetCellStyle(gopdf.CellStyle{
		BorderStyle: gopdf.BorderStyle{
			Top:      true,
			Width:    0.5,
			RGBColor: gopdf.RGBColor{R: 0, G: 0, B: 0},
		},
		FillColor: gopdf.RGBColor{R: 255, G: 255, B: 255},
		TextColor: gopdf.RGBColor{R: 0, G: 0, B: 0},
		Font:      "font1",
		FontSize:  10,
	})

	table.AddColumn("Order Id", 188, "center")
	table.AddColumn("Order Date", 188, "center")
	table.AddColumn("Revenue", 188, "center")

	for _, v := range content {
		table.AddRow([]string{v.OrderId, v.OrderDate, v.Revenue})
	}

	// Draw the table
	table.DrawTable()
	return
}
