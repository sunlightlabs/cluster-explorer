
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
    m0 := NewSymmetricMatrix(0)
    assertEqual(t, 0, m0.Size())

    m1 := NewSymmetricMatrix(1)
    m1.SetValue(0, 0, 1.1)
    assertEqual(t, 1, m1.Size())
    assertEqual(t, float32(1.1), m1.Value(0, 0))
    
    m3 := NewSymmetricMatrix(3)
    m3.SetValue(0, 0, 0.0)
    m3.SetValue(1, 0, 1.0)
    m3.SetValue(1, 1, 1.1)
    m3.SetValue(2, 0, 2.0)
    m3.SetValue(2, 1, 2.1)
    m3.SetValue(2, 2, 2.2)
    assertEqual(t, float32(0.0), m3.Value(0, 0))
    assertEqual(t, float32(1.0), m3.Value(1, 0))
    assertEqual(t, float32(1.1), m3.Value(1, 1))
    assertEqual(t, float32(2.0), m3.Value(2, 0))
    assertEqual(t, float32(2.1), m3.Value(2, 1))
    assertEqual(t, float32(2.2), m3.Value(2, 2))
}