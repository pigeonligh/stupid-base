package errorutil

import "errors"

var (
	ErrorDbSysCreateDbFails = errors.New("create database fails")

	ErrorDbSysDropDbFails = errors.New("drop database fails")

	ErrorDbSysOpenDbFails = errors.New("open database fails")

	ErrorDbSysCloseDbFails = errors.New("close database fails")

	ErrorDbSysDbNotSelected = errors.New("database not selected")

	ErrorDbSysTableExisted = errors.New("table existed")

	ErrorDbSysTableNotExisted = errors.New("table not existed")

	// some rules
	ErrorDbSysMaxAttrExceeded = errors.New("max attr nums 40 exceed")

	ErrorDbSysMaxNameExceeded = errors.New("max rel name size 24 exceed")

	ErrorDbSysPrimaryKeyCntExceed = errors.New("primary key cnt exceed")

	ErrorDbSysForeignKeyLenNotMatch = errors.New("foreign key len not match")

	ErrorDbSysForeignKeyExists = errors.New("foreign key exists")

	ErrorDbSysRelationOrAttrNotExists = errors.New("relation or attr not exists")
)
