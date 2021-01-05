package dbsys

import (
	"bytes"
	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"strconv"
	"strings"
	"time"
)

func strTo24ByteArray(name string) [24]byte {
	ret := [24]byte{}
	copy(ret[:], name)
	return ret
}

func ByteArray24tostr(array [24]byte) string {
	return strings.TrimSpace(string(bytes.Trim(array[:], string(byte(0)))))
}

func data2StringByTypes(data []byte, valueType types.ValueType) string {
	ret := ""
	switch valueType {
	case types.INT:
		val := *(*int)(types.ByteSliceToPointer(data))
		ret = strconv.Itoa(val)
	case types.FLOAT:
		val := *(*float64)(types.ByteSliceToPointer(data))
		ret = strconv.FormatFloat(val, 'g', 10, 64) // TODO: more dynamic float converting
	case types.VARCHAR:
		ret = string(data)
	case types.DATE:
		val := *(*int)(types.ByteSliceToPointer(data))
		unixTime := time.Unix(int64(val), 0)
		ret = unixTime.Format(time.RFC822)
	case types.BOOL:
		val := *(*bool)(types.ByteSliceToPointer(data))
		ret = strconv.FormatBool(val)
	}
	// NO ATTR return "" by default

	return strings.TrimSpace(string(bytes.Trim([]byte(ret), string(byte(0)))))

}
