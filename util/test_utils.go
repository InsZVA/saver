package util

import (
	"math/rand"
)

func RandomSlice(maxLength int) []byte {
	ret := make([]byte, 0, maxLength)
	for i := 0; i < maxLength; i++ {
		ret = append(ret, 'a'+byte(rand.Intn(26)))
	}
	return ret
}
