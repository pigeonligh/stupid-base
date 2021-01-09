/*
Copyright (c) 2020, pigeonligh.
*/

package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/pigeonligh/stupid-base/pkg/core/database"
)

func main() {
	db, _ := database.New()

	r := bufio.NewReader(os.Stdin)

	sqls := ""

	for {
		fmt.Println("")

		sql, err := r.ReadString('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println("Error: ", err)
			break
		}

		sqls = sqls + " " + sql
		sqls = strings.TrimRight(sqls, " \n")
		if len(sqls) == 0 {
			continue
		}
		if sqls[len(sqls)-1] == '\\' {
			sqls = sqls[0 : len(sqls)-1]
			continue
		}

		if err := db.Run(sqls); err != nil {
			fmt.Println("Error: ", err)
		}
		sqls = ""
	}
}
