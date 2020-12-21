/*
Copyright (c) 2020, pigeonligh.
*/

package main

import "github.com/pigeonligh/stupid-base/pkg/core"

func main() {
	db, _ := core.NewDatabase()
	db.Run("SELECT * FROM `table` WHERE a = 'abc'")
	db.Run("show databases")
	db.Run("create database `table`;")
}
