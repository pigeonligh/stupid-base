package types

const MaxNameSize = 24
const MaxAttrNums = 40
const MaxStringSize = 255

type ValueType = int

const (
	NO_ATTR ValueType = iota
	INT
	FLOAT
	DATE
	VARCHAR
	BOOL
)

var ValueTypeStringMap = map[ValueType]string{
	NO_ATTR: "NO_ATTR",
	INT:     "INT",
	FLOAT:   "FLOAT",
	DATE:    "DATE",
	VARCHAR: "VARCHAR",
	BOOL:    "BOOL",
}

type OpType = int

const (
	OpDefault OpType = iota
	OpCompEQ
	OpCompLT
	OpCompGT
	OpCompLE
	OpCompGE
	OpCompNE
	OpCompIS
	OpCompISNOT
	OpCompLIKE
	OpCompNO

	OpArithADD
	OpArithSUB
	OpArithMUL
	OpArithDIV
	OpArithMINUS
	OpArithNO

	OpLogicAND
	OpLogicOR
	OpLogicNOT
	//OpLogicNO don't know what is it
)

func IsOpComp(op OpType) bool {
	return op == OpDefault ||
		op == OpCompEQ ||
		op == OpCompLT ||
		op == OpCompGT ||
		op == OpCompLE ||
		op == OpCompGE ||
		op == OpCompNE ||
		op == OpCompIS ||
		op == OpCompISNOT
}

func IsOpLogic(op OpType) bool {
	return op == OpDefault || op == OpLogicAND || op == OpLogicOR || op == OpLogicNOT
}

type NodeType = int

const (
	NodeArith NodeType = iota
	NodeComp
	NodeLogic
	NodeConst
	NodeAttr
)

// enum class AggregationType {
// T_NONE = 0,
// T_AVG,
// T_SUM,
// T_MIN,
// T_MAX
// };

// enum class ConstraintType {
// PRIMARY_CONSTRAINT,
// FOREIGN_CONSTRAINT,
// CHECK_CONSTRAINT
// };
