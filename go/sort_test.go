
package main

import (
    "testing"
)


func TestSort(t *testing.T) {
    data := []int{2, 0, 3, 1, 4}
    SortInts(data)
    assertEqual(t, 0, data[0])
    assertEqual(t, 1, data[1])
    assertEqual(t, 2, data[2])
    assertEqual(t, 3, data[3])
    assertEqual(t, 4, data[4])
}

//type foo string

func TestInterfaceSort(t *testing.T) {
    data := []interface{}{"soaped", "foo", "spaz", "op"}
    SortInterfaces(data, func(a, b interface{}) bool { return len(a.(string)) < len(b.(string)) })
    assertEqual(t, "op", data[0])
    assertEqual(t, "foo", data[1])
    assertEqual(t, "spaz", data[2])
    assertEqual(t, "soaped", data[3])
}

