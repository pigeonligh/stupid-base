package parser

import (
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	log "github.com/pigeonligh/stupid-base/pkg/logutil"
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
	ValueSize int
	ValueType types.ValueType
	value     [64]byte
	str       string
}

func (v *Value) GE(c *Value) bool {
	if v.ValueType != c.ValueType {
		log.V(log.ExprLevel).Warningf("op Value type not match\n")
		return false
	}
	switch v.ValueType {
	case types.INT:
		return v.ToInt64() >= c.ToInt64()
	case types.FLOAT:
		return v.ToFloat64() >= c.ToFloat64()
	case types.STRING:
		return v.ToStr() >= c.ToStr()
	case types.DATE:
		return v.ToInt64() >= c.ToInt64()
	case types.VARCHAR:
		return v.ToStr() >= c.ToStr()
	case types.BOOL, types.NO_ATTR:
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
	case types.INT:
		return v.ToInt64() > c.ToInt64()
	case types.FLOAT:
		return v.ToFloat64() > c.ToFloat64()
	case types.STRING:
		return v.ToStr() > c.ToStr()
	case types.DATE:
		return v.ToInt64() > c.ToInt64()
	case types.VARCHAR:
		return v.ToStr() > c.ToStr()
	case types.BOOL, types.NO_ATTR:
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
	case types.INT:
		return v.ToInt64() <= c.ToInt64()
	case types.FLOAT:
		return v.ToFloat64() <= c.ToFloat64()
	case types.STRING:
		return v.ToStr() <= c.ToStr()
	case types.DATE:
		return v.ToInt64() <= c.ToInt64()
	case types.VARCHAR:
		return v.ToStr() <= c.ToStr()
	case types.BOOL, types.NO_ATTR:
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
	case types.INT:
		return v.ToInt64() < c.ToInt64()
	case types.FLOAT:
		return v.ToFloat64() < c.ToFloat64()
	case types.STRING:
		return v.ToStr() < c.ToStr()
	case types.DATE:
		return v.ToInt64() < c.ToInt64()
	case types.VARCHAR:
		return v.ToStr() < c.ToStr()
	case types.BOOL, types.NO_ATTR:
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
	case types.INT:
		return v.ToInt64() != c.ToInt64()
	case types.FLOAT:
		return v.ToFloat64() != c.ToFloat64()
	case types.STRING:
		return v.ToStr() != c.ToStr()
	case types.DATE:
		return v.ToInt64() != c.ToInt64()
	case types.VARCHAR:
		return v.ToStr() != c.ToStr()
	case types.BOOL:
		return v.ToBool() != c.ToBool()
	case types.NO_ATTR:
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
	case types.INT:
		return v.ToInt64() == c.ToInt64()
	case types.FLOAT:
		return v.ToFloat64() == c.ToFloat64()
	case types.STRING:
		return v.ToStr() == c.ToStr()
	case types.DATE:
		return v.ToInt64() == c.ToInt64()
	case types.VARCHAR:
		return v.ToStr() == c.ToStr()
	case types.BOOL:
		return v.ToBool() == c.ToBool()
	case types.NO_ATTR:
		return false
	}
	return false
}

func (v *Value) ToInt64() int {
	return *(*int)(unsafe.Pointer(&v.value))
}

func (v *Value) FromInt64(val int) {
	ptr := (*int)(types.ByteSliceToPointer(v.value[:]))
	*ptr = val
}

func NewValueFromInt64(val int) Value {
	ret := Value{ValueType: types.INT}
	ret.FromInt64(val)
	return ret
}

func (v *Value) ToFloat64() float64 {
	return *(*float64)(unsafe.Pointer(&v.value))
}

func (v *Value) FromFloat64(val float64) {
	ptr := (*float64)(types.ByteSliceToPointer(v.value[:]))
	*ptr = val
}

func NewValueFromFloat64(val float64) Value {
	ret := Value{ValueType: types.FLOAT}
	ret.FromFloat64(val)
	return ret
}

func (v *Value) ToBool() bool {
	return *(*bool)(unsafe.Pointer(&v.value))
}

func (v *Value) FromBool(val bool) {
	v.value = [64]byte{}
	ptr := (*bool)(types.ByteSliceToPointer(v.value[:]))
	*ptr = val
}

func NewValueFromBool(val bool) Value {
	ret := Value{ValueType: types.BOOL}
	ret.FromBool(val)
	return ret
}

func (v *Value) ToStr() string {
	return v.str
}

func (v *Value) FromStr(s string) {
	v.str = s
}

func NewValueFromStr(s string) Value {
	ret := Value{ValueType: types.STRING}
	ret.FromStr(s)
	return ret
}
