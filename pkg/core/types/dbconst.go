package types

type ValueType = int
const (
	NO_ATTR ValueType = iota
	INT
	FLOAT
	STRING
	DATE
	VARCHAR
	BOOL

)

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
	OpLogicNO
)

func IsOpComp(op OpType) bool{
	return op == OpCompEQ || op == OpCompLT || op == OpCompGT || op == OpCompLE || op == OpCompGE || op == OpCompNE || op == OpCompIS || op == OpCompISNOT
}

type NodeType = int
const (
	NodeArith NodeType = iota
	NodeComp
	NodeLogic
	NodeConst
	NodeAttr
)

//enum class AggregationType {
//T_NONE = 0,
//T_AVG,
//T_SUM,
//T_MIN,
//T_MAX
//};

//enum class ConstraintType {
//PRIMARY_CONSTRAINT,
//FOREIGN_CONSTRAINT,
//CHECK_CONSTRAINT
//};