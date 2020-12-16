package parser

import (
	"bytes"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
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
	Value     [types.MaxStringSize]byte
	ValueType types.ValueType
}

// similar to the functions in pkg/core/dbsys/datautils.go:21
// data2StringByTypes
func (v *Value) Format2String() string {
	ret := ""
	switch v.ValueType {
	case types.INT:
		val := *(*int)(types.ByteSliceToPointer(v.Value[:]))
		ret = strconv.Itoa(val)
	case types.FLOAT:
		val := *(*float64)(types.ByteSliceToPointer(v.Value[:]))
		ret = strconv.FormatFloat(val, 'g', 10, 64) // TODO: more dynamic float converting
	case types.STRING, types.VARCHAR:
		ret = string(v.Value[:])
	case types.DATE:
		val := *(*int)(types.ByteSliceToPointer(v.Value[:]))
		unixTime := time.Unix(int64(val), 0)
		ret = unixTime.Format(time.RFC822)
	case types.BOOL:
		val := *(*bool)(types.ByteSliceToPointer(v.Value[:]))
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
	return *(*int)(unsafe.Pointer(&v.Value))
}

func (v *Value) FromInt64(val int) {
	ptr := (*int)(types.ByteSliceToPointer(v.Value[:]))
	*ptr = val
}

func NewValueFromInt64(val int) Value {
	ret := Value{ValueType: types.INT}
	ret.FromInt64(val)
	return ret
}

func (v *Value) ToFloat64() float64 {
	return *(*float64)(unsafe.Pointer(&v.Value))
}

func (v *Value) FromFloat64(val float64) {
	ptr := (*float64)(types.ByteSliceToPointer(v.Value[:]))
	*ptr = val
}

func NewValueFromFloat64(val float64) Value {
	ret := Value{ValueType: types.FLOAT}
	ret.FromFloat64(val)
	return ret
}

func (v *Value) ToBool() bool {
	return *(*bool)(unsafe.Pointer(&v.Value))
}

func (v *Value) FromBool(val bool) {
	ptr := (*bool)(types.ByteSliceToPointer(v.Value[:]))
	*ptr = val
}

func NewValueFromBool(val bool) Value {
	ret := Value{ValueType: types.BOOL}
	ret.FromBool(val)
	return ret
}

func (v *Value) ToStr() string {
	return string(v.Value[:])
}

func (v *Value) FromStr(s string) {
	v.Value = [types.MaxStringSize]byte{}
	byteSlice := []byte(s)
	if len(byteSlice) > types.MaxStringSize {
		byteSlice = byteSlice[0:types.MaxStringSize]
	}
	copy(v.Value[:], byteSlice)
}

func NewValueFromStr(s string) Value {
	ret := Value{ValueType: types.STRING}
	ret.FromStr(s)
	return ret
}

func NewValueFromByteSlice(byteSlice []byte, valueType types.ValueType) Value {
	ret := Value{ValueType: valueType}
	copy(ret.Value[:], byteSlice)
	return ret
}
