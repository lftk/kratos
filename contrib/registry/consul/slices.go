package consul

import (
	"cmp"
	"sort"
)

func contains[S ~[]E, E comparable](s S, v E) bool {
	return index(s, v) >= 0
}

func containsFunc[S ~[]E, E any](s S, f func(E) bool) bool {
	return indexFunc(s, f) >= 0
}

func index[S ~[]E, E comparable](s S, v E) int {
	for i := range s {
		if v == s[i] {
			return i
		}
	}
	return -1
}

func indexFunc[S ~[]E, E any](s S, f func(E) bool) int {
	for i := range s {
		if f(s[i]) {
			return i
		}
	}
	return -1
}

func deleteFunc[S ~[]E, E any](s S, del func(E) bool) S {
	i := indexFunc(s, del)
	if i == -1 {
		return s
	}
	// Don't start copying elements until we find one to delete.
	for j := i + 1; j < len(s); j++ {
		if v := s[j]; !del(v) {
			s[i] = v
			i++
		}
	}
	return s[:i]
}

func uniq[S ~[]E, E cmp.Ordered](s S) S {
	if len(s) == 0 {
		return nil
	}
	out := make([]E, len(s))
	copy(out, s)
	sort.Slice(out, func(i, j int) bool {
		return out[i] < out[j]
	})
	uniq := out[:0]
	for _, x := range out {
		if len(uniq) == 0 || uniq[len(uniq)-1] != x {
			uniq = append(uniq, x)
		}
	}
	return uniq
}
