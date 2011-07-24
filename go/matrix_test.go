
package main

import (
    "testing"
)

func assertEqual(t *testing.T, a, b interface{}) {
	if a != b {
		t.Error(a, b)
	}
}

func TestMatrix(t *testing.T) {
    m2 := NewTriangleMatrix(2)
    m2.SetValue(1, 0, 1.0)
    assertEqual(t, 2, m2.Size())
    assertEqual(t, float32(1.0), m2.Value(1, 0))
    
    m4 := NewTriangleMatrix(4)
    m4.SetValue(1, 0, 1.0)
    m4.SetValue(2, 0, 2.0)
    m4.SetValue(2, 1, 2.1)
    m4.SetValue(3, 0, 3.0)
    m4.SetValue(3, 1, 3.1)
    m4.SetValue(3, 2, 3.2)

    assertEqual(t, 4, m4.Size())
    assertEqual(t, float32(1.0), m4.Value(1, 0))
    assertEqual(t, float32(2.0), m4.Value(2, 0))
    assertEqual(t, float32(2.1), m4.Value(2, 1))
    assertEqual(t, float32(3.0), m4.Value(3, 0))
    assertEqual(t, float32(3.1), m4.Value(3, 1))
    assertEqual(t, float32(3.2), m4.Value(3, 2))
}