package errorutil

import "errors"

var (
	ErrorDBSysCreateDBFails = errors.New("create database fails")

	ErrorDBSysDropDBFails = errors.New("drop database fails")

	ErrorDBSysOpenDBFails = errors.New("open database fails")

	ErrorDBSysCloseDBFails = errors.New("close database fails")

	ErrorDBSysDBNotSelected = errors.New("database not selected")

	ErrorDBSysTableExisted = errors.New("table existed")

	ErrorDBSysRelationNotExisted = errors.New("table not existed")

	ErrorDBSysAttrNotExisted = errors.New("table attribute not existed")

	// some rules
	ErrorDBSysMaxAttrExceeded = errors.New("max attr nums 40 exceed")

	ErrorDBSysMaxNameExceeded = errors.New("max rel name size 24 exceed")

	ErrorDBSysPrimaryKeyCntExceed = errors.New("primary key cnt exceed")

	ErrorDBSysPrimaryKeyDoNotExist = errors.New("primary key do not exist")

	// foreign key
	ErrorDBSysForeignKeyLenNotMatch = errors.New("foreign key len not match")

	ErrorDBSysForeignKeyExists = errors.New("foreign key exists")

	ErrorDBSysFkNotRefPk = errors.New("foreign key not reference primary key")

	ErrorDBSysCreateTableWithDupAttr = errors.New("attr duplicated")

	ErrorDBSysBigRecordNotSupported = errors.New("big record not supported")

	// index related
	ErrorDBSysColIndexAlreadyExisted = errors.New("column index already created")

	ErrorDBSysIndexNameAlreadyExisted = errors.New("index name already created")

	ErrorDBSysIndexNameNotExisted = errors.New("index name not existed")

	ErrorDBSysInvalidIndexName = errors.New("invalid idx name > 24")
)
