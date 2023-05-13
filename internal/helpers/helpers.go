package helpers

func ItemInSlice[T comparable](item T, slice []T) bool {
	for _, v := range slice {
		if v == item {
			return true
		}
	}

	return false
}

func CopySliceElems[T any](source []T, target *[]any) {
	for _, v := range source {
		*target = append(*target, v)
	}
}
