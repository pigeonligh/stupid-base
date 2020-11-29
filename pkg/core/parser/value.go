package parser

import (
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	log "github.com/pigeonligh/stupid-base/pkg/logutil"
	"unsafe"
)

//type ConvertValue interface {
//	toInt64() int
//	fromInt64(val int)
//	toFloat64() float64
//	fromFloat64(val float64)
//	toBool() bool
//	fromBool(val bool)
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
		return v.toInt64() >= c.toInt64()
	case types.FLOAT:
		return v.toFloat64() >= c.toFloat64()
	case types.STRING:
		return v.toStr() >= c.toStr()
	case types.DATE:
		return v.toInt64() >= c.toInt64()
	case types.VARCHAR:
		return v.toStr() >= c.toStr()
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
		return v.toInt64() > c.toInt64()
	case types.FLOAT:
		return v.toFloat64() > c.toFloat64()
	case types.STRING:
		return v.toStr() > c.toStr()
	case types.DATE:
		return v.toInt64() > c.toInt64()
	case types.VARCHAR:
		return v.toStr() > c.toStr()
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
		return v.toInt64() <= c.toInt64()
	case types.FLOAT:
		return v.toFloat64() <= c.toFloat64()
	case types.STRING:
		return v.toStr() <= c.toStr()
	case types.DATE:
		return v.toInt64() <= c.toInt64()
	case types.VARCHAR:
		return v.toStr() <= c.toStr()
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
		return v.toInt64() < c.toInt64()
	case types.FLOAT:
		return v.toFloat64() < c.toFloat64()
	case types.STRING:
		return v.toStr() < c.toStr()
	case types.DATE:
		return v.toInt64() < c.toInt64()
	case types.VARCHAR:
		return v.toStr() < c.toStr()
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
		return v.toInt64() != c.toInt64()
	case types.FLOAT:
		return v.toFloat64() != c.toFloat64()
	case types.STRING:
		return v.toStr() != c.toStr()
	case types.DATE:
		return v.toInt64() != c.toInt64()
	case types.VARCHAR:
		return v.toStr() != c.toStr()
	case types.BOOL:
		return v.toBool() != c.toBool()
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
		return v.toInt64() == c.toInt64()
	case types.FLOAT:
		return v.toFloat64() == c.toFloat64()
	case types.STRING:
		return v.toStr() == c.toStr()
	case types.DATE:
		return v.toInt64() == c.toInt64()
	case types.VARCHAR:
		return v.toStr() == c.toStr()
	case types.BOOL:
		return v.toBool() == c.toBool()
	case types.NO_ATTR:
		return false
	}
	return false
}


func (v *Value) toInt64() int {
	return *(*int)(unsafe.Pointer(&v.value))
}

func (v *Value) fromInt64(val int)  {
	ptr := (*int)(types.ByteSliceToPointer(v.value[:]))
	*ptr = val
}

func (v *Value) toFloat64() float64 {
	return *(*float64)(unsafe.Pointer(&v.value))
}

func (v *Value) fromFloat64(val float64)  {
	ptr := (*float64)(types.ByteSliceToPointer(v.value[:]))
	*ptr = val
}

func (v *Value) toBool() bool {
	return *(*bool)(unsafe.Pointer(&v.value))
}

func (v *Value) fromBool(val bool)  {
	v.value = [64]byte{}
	ptr := (*bool)(types.ByteSliceToPointer(v.value[:]))
	*ptr = val
}

func (v* Value) toStr() string{
	return v.str
}

func (v* Value) fromStr(s string){
	v.str = s
}
