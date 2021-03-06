package types

import (
	"bytes"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/pigeonligh/stupid-base/pkg/errorutil"
)

type Value struct {
	Value     [MaxStringSize]byte
	ValueType ValueType
}

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

func String2Value(str string, size int, target ValueType) (Value, error) {
	val := NewValueFromEmpty()
	if str == MagicNullString {
		return val, nil
	}
	switch target {
	case VARCHAR:
		if len(str) > size {
			return val, errorutil.ErrorDBSysStringExceedLength
		}
		val.FromStr(str)
	case BOOL:
		b, err := strconv.ParseBool(str)
		if err != nil {
			return val, err
		}
		val.FromBool(b)
	case DATE:
		t, err := time.Parse("2006-1-2", str)
		if err != nil {
			return val, err
		}
		val.FromInt64(int(t.Unix()))
	case INT:
		i, err := strconv.ParseInt(str, 10, size*8)
		if err != nil {
			return val, err
		}
		val.FromInt64(int(i))
	case FLOAT:
		f, err := strconv.ParseFloat(str, size*8)
		if err != nil {
			return val, err
		}
		val.FromFloat64(f)
	}
	return val, nil
}

// AdaptToType set no attr means convert fails
func (v *Value) AdaptToType(target ValueType) {
	if v.ValueType == VARCHAR && target != VARCHAR {
		tmp, err := String2Value(v.ToStr(), ValueTypeDefaultSize[target], target)
		if err != nil {
			v.ValueType = NO_ATTR
			return
		}
		*v = tmp
		return
	}
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
		ret = unixTime.Format("2006-1-2")
	case BOOL:
		val := *(*bool)(ByteSliceToPointer(v.Value[:]))
		ret = strconv.FormatBool(val)
	}
	// NO ATTR return "" by default

	return strings.TrimSpace(string(bytes.Trim([]byte(ret), string(byte(0)))))
}

func (v *Value) GE(c *Value) bool {
	c.AdaptToType(v.ValueType)
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
	c.AdaptToType(v.ValueType)
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
	c.AdaptToType(v.ValueType)
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
	c.AdaptToType(v.ValueType)
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
	c.AdaptToType(v.ValueType)
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
	c.AdaptToType(v.ValueType)

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
	return *(*int)(ByteSliceToPointer(v.Value[:]))
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
	return v.Format2String()
}

func (v *Value) FromStr(s string) {
	v.Value = [MaxStringSize]byte{}
	byteSlice := []byte(s)
	if len(byteSlice) > MaxStringSize {
		byteSlice = byteSlice[0:MaxStringSize]
	}
	copy(v.Value[0:len(byteSlice)], byteSlice)
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

func NewValueFromEmpty() Value {
	return Value{
		Value:     [255]byte{},
		ValueType: NO_ATTR,
	}
}

func NewValueFromDate(time time.Time) Value {
	Value := NewValueFromInt64(int(time.Unix()))
	Value.ValueType = DATE
	return Value
}
