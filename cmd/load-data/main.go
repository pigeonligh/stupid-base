/*
Copyright (c) 2020, pigeonligh.
*/

package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/pigeonligh/stupid-base/pkg/core/database"
)

func main() {
	db, _ := database.New()

	switch len(os.Args) {
	case 2:
		if err := db.Run("create database testdb;"); err != nil {
			fmt.Println("Error:", err)
			os.Exit(1)
		}
		if err := db.Run("use testdb;"); err != nil {
			fmt.Println("Error:", err)
			os.Exit(1)
		}

		for tableName, tableConfig := range configs {
			fmt.Println("create", tableName)

			attrs := ""
			for _, attr := range tableConfig {
				attrs = attrs + attr
			}
			sql := "create table " + tableName + " ( " + attrs + " );"

			if err := db.Run(sql); err != nil {
				fmt.Println("Error:", err)
				os.Exit(1)
			}
		}

		/*
			for _, sql := range afterCommand {
				if err := db.Run(sql); err != nil {
					fmt.Println("Error:", err)
					os.Exit(1)
				}
			}
		*/

		for _, tableName := range configOrder {
			fmt.Println("insert", tableName)

			filename := os.Args[1] + "/" + tableName + ".tbl.csv"
			if tableName == "supplier" {
				filename = os.Args[1] + "/" + tableName + ".tbl"
			}
			data, err := ioutil.ReadFile(filename)
			if err != nil {
				fmt.Println("Error:", err)
				os.Exit(1)
			}

			lines := strings.Split(string(data), "\n")
			count := 0
			for i, line := range lines {
				if i == 0 && tableName != "supplier" {
					continue
				}
				if len(strings.TrimSpace(line)) == 0 {
					continue
				}

				row := strings.Split(line, "|")

				values := ""
				for _, value := range row {
					if values != "" {
						values = values + ", "
					}
					values = values + "'" + strings.TrimSpace(value) + "'"
				}

				sql := "insert into " + tableName + " values ( " + values + " )"

				err := db.Run(sql)
				if err == nil {
					count++
				}
			}
			fmt.Println("insert", count)
		}

		db.Run("show tables;")

	default:
		fmt.Println("unknown parameters")
		os.Exit(1)
	}
}
