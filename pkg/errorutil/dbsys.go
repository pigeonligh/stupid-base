package errorutil

import "errors"

var (
	// db operation
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

	ErrorDBSysMaxNameExceeded = errors.New("name length exceed size 24")

	ErrorDBSysPrimaryKeyCntExceed = errors.New("primary key cnt exceed")

	ErrorDBSysPrimaryKeyDoNotExist = errors.New("primary key do not exist")

	// foreign key
	ErrorDBSysForeignKeyRefSelf = errors.New("foreign key src relation reference self")

	ErrorDBSysForeignKeyLenNotMatch = errors.New("foreign key len not match")

	ErrorDBSysForeignKeyExists = errors.New("foreign key exists")

	ErrorDBSysForeignKeyNotExists = errors.New("foreign key not exists")

	ErrorDBSysFkValueNotInPk = errors.New("foreign key value not in pk")

	ErrorDBSysFkTypeNotMatchPk = errors.New("foreign key type not match primary key")

	ErrorDBSysCreateTableWithDupAttr = errors.New("attr duplicated")

	ErrorDBSysBigRecordNotSupported = errors.New("big record not supported")

	// index related
	ErrorDBSysColIndexAlreadyExisted = errors.New("column index already created")

	ErrorDBSysIndexNameAlreadyExisted = errors.New("index name already created")

	ErrorDBSysIndexNameNotExisted = errors.New("index name not existed")

	ErrorDBSysInvalidIndexName = errors.New("invalid idx name > 24")

	// primary key
	ErrorDBSysDuplicatedKeysFound = errors.New("duplicated keys found")

	ErrorDBSysIsNotPrimaryKeys = errors.New("is not primary keys")

	// insert rules
	ErrorDBSysInsertValueTypeNotMatch = errors.New("insert value type not match")
)
