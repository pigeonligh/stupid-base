package errorutil

import "errors"

var (
	// db operation
	ErrorDBSysCreateDBFails = errors.New("create database fails")

	ErrorDBSysDropDBFails = errors.New("drop database fails")

	ErrorDBSysOpenDBFails = errors.New("open database fails")

	ErrorDBSysCloseDBFails = errors.New("close database fails")

	ErrorDBSysDBNotSelected = errors.New("database not selected")

	ErrorDBSysRelationExisted = errors.New("table existed")

	ErrorDBSysRelationNotExisted = errors.New("table not existed")

	ErrorDBSysAttrNotExisted = errors.New("table attribute not existed")

	ErrorDBSysAttrExisted = errors.New("table attribute existed")

	ErrorDBSysDuplicatedAttrsFound = errors.New("duplicated attrs found")

	// some rules
	ErrorDBSysMaxAttrExceeded = errors.New("max attr nums 40 exceed")

	ErrorDBSysMaxNameExceeded = errors.New("name length exceed size 24")

	ErrorDBSysPrimaryKeyCntExceed = errors.New("primary key cnt exceed")

	ErrorDBSysPrimaryKeyDoNotExist = errors.New("primary key do not exist")

	// foreign key
	ErrorDBSysForeignKeyConstraintNotMatch = errors.New("foreign key constraint not match while deleting")

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

	// update rules
	ErrorDBSysUpdateValueTypeNotMatch = errors.New("update value type not match")

	ErrorDBSysNullConstraintViolated = errors.New("null constraint violated")

	// add column
	ErrorDBSysAddComplicateColumnNotSupported = errors.New("add complicate col not supported")

	// remove column
	ErrorDBSysCannotRemoveLastColumn = errors.New("cannot remove last column")

	ErrorDBSysCannotRemovePrimaryColumn = errors.New("cannot remove primary column with multiple column define on table")

	ErrorDBSysCannotRemoveForeignKeyCol = errors.New("cannot remove column which has fk")

	// change
	ErrorDBSysCannotChangePkFkColumn = errors.New("cannot change primary columnn")
)
