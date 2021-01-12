package types

type ClusterType int

const (
	NoneCluster ClusterType = iota
	MinCluster
	MaxCluster
	SumCluster
	AverageCluster
)
