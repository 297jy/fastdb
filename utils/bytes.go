package utils

func LongestPrefix(k1, k2 []byte) int {
	num := min(len(k1), len(k2))
	var i int
	for i = 0; i < num; i++ {
		if k1[i] != k2[i] {
			break
		}
	}
	return i
}

func Concat(a, b []byte) []byte {
	c := make([]byte, len(a)+len(b))
	copy(c, a)
	copy(c[len(a):], b)
	return c
}
