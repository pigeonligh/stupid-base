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



int
RM_FileScan::openScan(const RM_FileHandle &fileHandle, AttrType attrType, int attrLength, int attrOffset, CompOp compOp,
void *value) {
if (compOp != CompOp::NO_OP) {
Expr *left = new Expr();
left->attrInfo.attrSize = attrLength;
left->attrInfo.attrOffset = attrOffset;
left->attrInfo.attrType = attrType;
left->attrInfo.notNull = false;
left->nodeType = NodeType::ATTR_NODE;

Expr *right;
if (value != nullptr) {
switch (attrType) {
case AttrType::INT:
case AttrType::DATE: {
int i = *reinterpret_cast<int *>(value);
right = new Expr(i);
break;
}
case AttrType::FLOAT: {
float f = *reinterpret_cast<float *>(value);
right = new Expr(f);
break;
}
case AttrType::STRING: {
char *s = reinterpret_cast<char *>(value);
right = new Expr(s);
break;
}
case AttrType::BOOL: {
bool b = *reinterpret_cast<bool *>(value);
right = new Expr(b);
break;
}
case AttrType::NO_ATTR:
case AttrType::VARCHAR:
right = new Expr();
break;
}
} else {
right = new Expr();
}

Expr *condition = new Expr(left, compOp, right);
return openScan(fileHandle, condition, "");
} else {
return openScan(fileHandle, nullptr, "");
}
}