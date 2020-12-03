package dbsys

func strTo24ByteArray(name string) [24]byte {
	ret := [24]byte{}
	copy(ret[:], name)
	return ret
}
