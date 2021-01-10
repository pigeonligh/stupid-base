package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/pigeonligh/stupid-base/pkg/core/database"
)

var LivePrefixState struct {
	LivePrefix string
	IsEnable   bool
}

var query string = ""

var history []string = make([]string, 0)

var db *database.Database

func executor(in string) {
	sql := strings.Trim(query+in, " \n;")
	if strings.HasSuffix(in, ";") || sql == "" {
		query = query + in
		LivePrefixState.IsEnable = false
		LivePrefixState.LivePrefix = in

		solve(sql)

		history = append(history, query)
		query = ""
		return
	}
	query = query + in + " "
	LivePrefixState.LivePrefix = "... "
	LivePrefixState.IsEnable = true
}

func changeLivePrefix() (string, bool) {
	return LivePrefixState.LivePrefix, LivePrefixState.IsEnable
}

func solve(sqls string) {
	if sqls == "exit" {
		os.Exit(0)
	}

	if err := db.Run(sqls); err != nil {
		fmt.Println("Error: ", err)
	}
}
