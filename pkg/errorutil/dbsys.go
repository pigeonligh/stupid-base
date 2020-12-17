package errorutil

import "errors"

var (
	ErrorDbSysCreateDbFails = errors.New("create database fails")

	ErrorDbSysDropDbFails = errors.New("drop database fails")

	ErrorDbSysOpenDbFails = errors.New("open database fails")

	ErrorDbSysCloseDbFails = errors.New("close database fails")

	ErrorDbSysDbNotSelected = errors.New("database not selected")

	ErrorDbSysTableExisted = errors.New("table existed")

	ErrorDbSysRelationNotExisted = errors.New("table not existed")

	ErrorDbSysAttrNotExisted = errors.New("table attribute not existed")

	// some rules
	ErrorDbSysMaxAttrExceeded = errors.New("max attr nums 40 exceed")

	ErrorDbSysMaxNameExceeded = errors.New("max rel name size 24 exceed")

	ErrorDbSysPrimaryKeyCntExceed = errors.New("primary key cnt exceed")

	ErrorDbSysPrimaryKeyDoNotExist = errors.New("primary key do not exist")

	ErrorDbSysForeignKeyLenNotMatch = errors.New("foreign key len not match")

	ErrorDbSysForeignKeyExists = errors.New("foreign key exists")

	ErrorDbSysCreateTableWithDupAttr = errors.New("attr duplicated")

	ErrorDbSysBigRecordNotSupported = errors.New("big record not supported")

	// index related
	ErrorDbSysColIndexAlreadyExisted = errors.New("column index already created")

	ErrorDbSysIndexNameAlreadyExisted = errors.New("index name already created")

	ErrorDbSysIndexNameNotExisted = errors.New("index name not existed")

	ErrorDbSysInvalidIndexName = errors.New("invalid idx name > 24")
)
