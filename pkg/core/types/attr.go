package types

type AttrInfo struct {
	AttrSize             int // used by expr::NodeAttr
	AttrOffset           int // used by expr::NodeAttr
	AttrType             ValueType
	IndexNo              int  // used by system manager
	ConstraintRID        RID  // used by system manager, deprecated
	NullAllowed          bool // used by system manager
	IsPrimary            bool // used by system manager
	HasForeignConstraint bool // will be checked if necessary
}
