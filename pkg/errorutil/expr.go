package errorutil

import "errors"

var (
	ErrorExprInvalidComparison = errors.New("invalid comparison between two operands")

	ErrorExprNonNullNotCalculated = errors.New("non null value not calculated")

	ErrorExprNodeNotImplemented = errors.New("exp node type not implemented")

	// compare op
	ErrorExprNodeCompViolateIsNullSyntax = errors.New("comp expr violate is null syntax")

	// logic op
	ErrorExprNodeLogicWithNonLogicOp = errors.New("node logic with non logic op")

	ErrorExprIsNotLogicComputable = errors.New("expr is not logic computable")

	ErrorExprBinaryOpWithNilChild = errors.New("binary operator must have two operands")

	ErrorExprUnaryOpWithNilRightChild = errors.New("unary operator with nil right child")

	ErrorExprUnaryOpWithNonNilLeftChild = errors.New("unary operator wtih non nil left child")
)
