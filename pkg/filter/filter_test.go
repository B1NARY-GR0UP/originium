package filter

import (
	"strconv"
	"testing"
)

func TestFalsePositiveRate(t *testing.T) {
	n := 1000
	p := 0.01
	bf := New(n, p)

	for i := 0; i < n; i++ {
		bf.Add(strconv.Itoa(i))
	}

	falsePositives := 0
	testSize := 10000

	for i := n; i < n+testSize; i++ {
		if bf.Contains(strconv.Itoa(i)) {
			falsePositives++
		}
	}

	actualP := float64(falsePositives) / float64(testSize)
	t.Log(actualP)
}
