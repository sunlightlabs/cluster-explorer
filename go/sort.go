
package main


import (
    "sort"
)

type sortableInts []int

func (s sortableInts) Len() int {
    return len(s)
}

func (s sortableInts) Swap(i, j int) {
    temp := s[i]
    s[i] = s[j]
    s[j] = temp
}

func (s sortableInts) Less(i, j int) bool {
    return s[i] < s[j]
}

func SortInts(data []int) {
    sort.Sort(sortableInts(data))
}



type sortableInterfaces struct {
    less func(a, b interface{}) bool
    data []interface{}
}

func (s sortableInterfaces) Len() int {
    return len(s.data)
}

func (s sortableInterfaces) Swap(i, j int) {
    temp := s.data[i]
    s.data[i] = s.data[j]
    s.data[j] = temp
}

func (s sortableInterfaces) Less(i, j int) bool {
    return s.less(s.data[i], s.data[j])
}

func SortInterfaces(data []interface{}, less func(a, b interface{}) bool) {
    sort.Sort(sortableInterfaces{less, data})
}

