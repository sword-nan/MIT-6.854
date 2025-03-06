package raft

type MatchedIndex []int

func (m MatchedIndex) Len() int {
	return len(m)
}

func (m MatchedIndex) Swap(i, j int) {
	m[i], m[j] = m[j], m[i]
}

func (m MatchedIndex) Less(i, j int) bool {
	return m[i] > m[j]
}

type Pair struct {
	start int
	end   int
}
