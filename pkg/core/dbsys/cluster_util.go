package dbsys

import (
	"strconv"

	"github.com/pigeonligh/stupid-base/pkg/core/types"
	"github.com/pigeonligh/stupid-base/pkg/errorutil"
)

func calcClusterValues(
	values []string,
	valueType types.ValueType,
	clusterType types.ClusterType,
) (string, error) {
	var result string

	switch valueType {
	case types.INT:
		var sum int = 0
		var min int = 0
		var max int = 0
		var count int = 0

		for i, str := range values {
			value, err := strconv.Atoi(str)
			if err != nil {
				return "", err
			}
			if i == 0 {
				min = value
				max = value
			}
			sum += value
			if min > value {
				min = value
			}
			if max < value {
				max = value
			}
			count++
		}

		switch clusterType {
		case types.MinCluster:
			result = strconv.Itoa(min)

		case types.MaxCluster:
			result = strconv.Itoa(max)

		case types.SumCluster:
			result = strconv.Itoa(sum)

		case types.AverageCluster:
			result = strconv.FormatFloat(float64(sum)/float64(count), 'G', 10, 64)
		}

	case types.FLOAT:
		var sum float64 = 0
		var min float64 = 0
		var max float64 = 0
		var count int = 0

		for i, str := range values {
			value, err := strconv.ParseFloat(str, 64)
			if err != nil {
				return "", err
			}
			if i == 0 {
				min = value
				max = value
			}
			sum += value
			if min > value {
				min = value
			}
			if max < value {
				max = value
			}
			count++
		}

		switch clusterType {
		case types.MinCluster:
			result = strconv.FormatFloat(min, 'G', 10, 64)

		case types.MaxCluster:
			result = strconv.FormatFloat(max, 'G', 10, 64)

		case types.SumCluster:
			result = strconv.FormatFloat(sum, 'G', 10, 64)

		case types.AverageCluster:
			result = strconv.FormatFloat(float64(sum)/float64(count), 'G', 10, 64)
		}
	}
	return result, nil
}

func calcCluster(
	valuesList []types.CalculatedValuesType,
	key types.SimpleAttr,
	attrType types.ValueType,
	ctype types.ClusterType,
) (string, error) {
	if attrType != types.INT && attrType != types.FLOAT {
		return "", errorutil.ErrorUndefinedBehaviour
	}

	list := []string{}
	for _, values := range valuesList {
		if value, found := values[key]; found {
			list = append(list, value)
		} else {
			return "", errorutil.ErrorColNotFound
		}
	}

	return calcClusterValues(list, attrType, ctype)
}
