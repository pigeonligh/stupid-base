package index

func compareBytes(attr1, attr2 []byte) int {
	// TODO: need to be compared based on the origin type

	if len(attr1) > len(attr2) {
		return -compareBytes(attr2, attr1)
	}
	compareLength := len(attr1)
	for i := 0; i < compareLength; i++ {
		if attr1[i] == attr2[i] {
			continue
		}
		if attr1[i] < attr2[i] {
			return 1
		}
		return -1
	}
	if len(attr1) < len(attr2) {
		return 1
	}
	return 0
}
