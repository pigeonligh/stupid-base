package types

import (
	"bytes"
	log "github.com/pigeonligh/stupid-base/pkg/logutil"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

//type ConvertValue interface {
//	ToInt64() int
//	FromInt64(val int)
//	ToFloat64() float64
//	FromFloat64(val float64)
//	ToBool() bool
//	FromBool(val bool)
//}

type Value struct {
	Value     [MaxStringSize]byte
	ValueType ValueType
}

//const (
//	NO_ATTR ValueType = iota
//	INT
//	FLOAT
//	DATE
//	VARCHAR
//	BOOL
//)
func CheckIfValueTypeCompatible(l, r ValueType) bool {
	if l == INT {
		return r == INT || r == FLOAT || r == DATE
	}
	if l == FLOAT {
		return r == INT || r == FLOAT
	}
	if l == DATE {
		return r == INT || r == DATE
	}
	return l == r
}

// AdaptToType set no attr means convert fails
func (v *Value) AdaptToType(target ValueType) {
	switch target {
	case INT:
		if v.ValueType == FLOAT {
			v.FromInt64(int(v.ToFloat64()))
			return
		}
		if v.ValueType == INT {
			return
		}
	case FLOAT:
		if v.ValueType == FLOAT {
			return
		}
		if v.ValueType == INT {
			v.FromFloat64(float64(v.ToInt64()))
			return
		}
	case DATE:
		if v.ValueType == INT || v.ValueType == DATE {
			v.ValueType = DATE
			return
		}
	case VARCHAR:
		if v.ValueType == VARCHAR {
			return
		}
	case BOOL:
		if v.ValueType == BOOL {
			return
		}
	}
	v.ValueType = NO_ATTR
}

// similar to the functions in pkg/core/dbsys/datautils.go:21
// data2StringByTypes
func (v *Value) Format2String() string {
	ret := ""
	switch v.ValueType {
	case INT:
		val := *(*int)(ByteSliceToPointer(v.Value[:]))
		ret = strconv.Itoa(val)
	case FLOAT:
		val := *(*float64)(ByteSliceToPointer(v.Value[:]))
		ret = strconv.FormatFloat(val, 'g', 10, 64) // TODO: more dynamic float converting
	case VARCHAR:
		ret = string(v.Value[:])
	case DATE:
		val := *(*int)(ByteSliceToPointer(v.Value[:]))
		unixTime := time.Unix(int64(val), 0)
		ret = unixTime.Format(time.RFC822)
	case BOOL:
		val := *(*bool)(ByteSliceToPointer(v.Value[:]))
		ret = strconv.FormatBool(val)
	}
	// NO ATTR return "" by default

	return strings.TrimSpace(string(bytes.Trim([]byte(ret), string(byte(0)))))
}

func (v *Value) GE(c *Value) bool {
	if v.ValueType != c.ValueType {
		log.V(log.ExprLevel).Warningf("op Value type not match\n")
		return false
	}
	switch v.ValueType {
	case INT:
		return v.ToInt64() >= c.ToInt64()
	case FLOAT:
		return v.ToFloat64() >= c.ToFloat64()
	case DATE:
		return v.ToInt64() >= c.ToInt64()
	case VARCHAR:
		return v.ToStr() >= c.ToStr()
	case BOOL, NO_ATTR:
		return false
	}
	return false
}

func (v *Value) GT(c *Value) bool {
	if v.ValueType != c.ValueType {
		log.V(log.ExprLevel).Warningf("op Value type not match\n")
		return false
	}
	switch v.ValueType {
	case INT:
		return v.ToInt64() > c.ToInt64()
	case FLOAT:
		return v.ToFloat64() > c.ToFloat64()
	case DATE:
		return v.ToInt64() > c.ToInt64()
	case VARCHAR:
		return v.ToStr() > c.ToStr()
	case BOOL, NO_ATTR:
		return false
	}
	return false
}

func (v *Value) LE(c *Value) bool {
	if v.ValueType != c.ValueType {
		log.V(log.ExprLevel).Warningf("op Value type not match\n")
		return false
	}
	switch v.ValueType {
	case INT:
		return v.ToInt64() <= c.ToInt64()
	case FLOAT:
		return v.ToFloat64() <= c.ToFloat64()
	case DATE:
		return v.ToInt64() <= c.ToInt64()
	case VARCHAR:
		return v.ToStr() <= c.ToStr()
	case BOOL, NO_ATTR:
		return false
	}
	return false
}

func (v *Value) LT(c *Value) bool {
	if v.ValueType != c.ValueType {
		log.V(log.ExprLevel).Warningf("op Value type not match\n")
		return false
	}
	switch v.ValueType {
	case INT:
		return v.ToInt64() < c.ToInt64()
	case FLOAT:
		return v.ToFloat64() < c.ToFloat64()
	case DATE:
		return v.ToInt64() < c.ToInt64()
	case VARCHAR:
		return v.ToStr() < c.ToStr()
	case BOOL, NO_ATTR:
		return false
	}
	return false
}

func (v *Value) NE(c *Value) bool {
	if v.ValueType != c.ValueType {
		log.V(log.ExprLevel).Warningf("op Value type not match\n")
		return false
	}
	switch v.ValueType {
	case INT:
		return v.ToInt64() != c.ToInt64()
	case FLOAT:
		return v.ToFloat64() != c.ToFloat64()
	case DATE:
		return v.ToInt64() != c.ToInt64()
	case VARCHAR:
		return v.ToStr() != c.ToStr()
	case BOOL:
		return v.ToBool() != c.ToBool()
	case NO_ATTR:
		return false
	}
	return false
}

func (v *Value) EQ(c *Value) bool {
	if v.ValueType != c.ValueType {
		log.V(log.ExprLevel).Warningf("op Value type not match\n")
		return false
	}
	switch v.ValueType {
	case INT:
		return v.ToInt64() == c.ToInt64()
	case FLOAT:
		return v.ToFloat64() == c.ToFloat64()
	case DATE:
		return v.ToInt64() == c.ToInt64()
	case VARCHAR:
		return v.ToStr() == c.ToStr()
	case BOOL:
		return v.ToBool() == c.ToBool()
	case NO_ATTR:
		return false
	}
	return false
}

func (v *Value) ToInt64() int {
	return *(*int)(unsafe.Pointer(&v.Value))
}

func (v *Value) FromInt64(val int) {
	ptr := (*int)(ByteSliceToPointer(v.Value[:]))
	*ptr = val
	v.ValueType = INT
}

func NewValueFromInt64(val int) Value {
	ret := Value{ValueType: INT}
	ret.FromInt64(val)
	return ret
}

func (v *Value) ToFloat64() float64 {
	return *(*float64)(unsafe.Pointer(&v.Value))
}

func (v *Value) FromFloat64(val float64) {
	ptr := (*float64)(ByteSliceToPointer(v.Value[:]))
	*ptr = val
	v.ValueType = FLOAT
}

func NewValueFromFloat64(val float64) Value {
	ret := Value{ValueType: FLOAT}
	ret.FromFloat64(val)
	return ret
}

func (v *Value) ToBool() bool {
	return *(*bool)(unsafe.Pointer(&v.Value))
}

func (v *Value) FromBool(val bool) {
	ptr := (*bool)(ByteSliceToPointer(v.Value[:]))
	*ptr = val
	v.ValueType = BOOL
}

func NewValueFromBool(val bool) Value {
	ret := Value{ValueType: BOOL}
	ret.FromBool(val)
	return ret
}

func (v *Value) ToStr() string {
	return string(v.Value[:])
}

func (v *Value) FromStr(s string) {
	v.Value = [MaxStringSize]byte{}
	byteSlice := []byte(s)
	if len(byteSlice) > MaxStringSize {
		byteSlice = byteSlice[0:MaxStringSize]
	}
	copy(v.Value[:], byteSlice)
	v.ValueType = VARCHAR
}

func NewValueFromStr(s string) Value {
	ret := Value{ValueType: VARCHAR}
	ret.FromStr(s)
	return ret
}

func NewValueFromByteSlice(byteSlice []byte, valueType ValueType) Value {
	ret := Value{ValueType: valueType}
	copy(ret.Value[:], byteSlice)
	return ret
}
