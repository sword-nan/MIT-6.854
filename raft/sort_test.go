package raft

import (
	"fmt"
	"sort"
	"testing"
)

func TestSort(t *testing.T) {
	var x = []int{1, 2, 3, 4, 5}
	sort.Sort(MatchedIndex(x))
	fmt.Println(x)
}
