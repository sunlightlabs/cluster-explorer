
package main

import (
    "testing"
    "fmt"
)

func newTestMatrix() (m TriangleMatrix) {
    m = NewTriangleMatrix(4)
    m.SetValue(1, 0, 1.0)
    m.SetValue(2, 0, 2.0)
    m.SetValue(2, 1, 2.1)
    m.SetValue(3, 0, 3.0)
    m.SetValue(3, 1, 3.1)
    m.SetValue(3, 2, 3.2)
    return
}

func TestCluster(t *testing.T) {    
    m := newTestMatrix()
    a := NewAssignment(4)

    for i := 0; i <= 3; i++ {
        for j := 0; j < i; j++ {
            assertEqual(t, false, a.IsSameCluster(i, j))
        }
    }

    for i, j, found := MinLink(m, a); found; i, j, found = MinLink(m, a) {
        a.Merge(i, j)
    }
    
    for i := 0; i <= 3; i++ {
        for j := 0; j < i; j++ {
            assertEqual(t, true, a.IsSameCluster(i, j))
        }
    }
}

func TestStepwise(t *testing.T) {    
    m := newTestMatrix()
    a := NewAssignment(4)

    for i := 0; i <= 3; i++ {
        for j := 0; j < i; j++ {
            assertEqual(t, false, a.IsSameCluster(i, j))
        }
    }

    var i, j int
    var found bool
    
    i, j, found = MinLink(m, a)
    assertEqual(t, true, found)
    assertEqual(t, 1, i)
    assertEqual(t, 0, j)
    a.Merge(i, j)
    
    i, j, found = MinLink(m, a)
    assertEqual(t, true, found)
    assertEqual(t, 2, i)
    assertEqual(t, 0, j)
    a.Merge(i, j)
    
    i, j, found = MinLink(m, a)
    assertEqual(t, true, found)
    assertEqual(t, 3, i)
    assertEqual(t, 0, j)
    a.Merge(i, j)

    i, j, found = MinLink(m, a)
    assertEqual(t, false, found)
}

func TestJSONOutput(t *testing.T) {
    m := newTestMatrix()
    a := NewAssignment(4)
 
    for n := 0; n < 5; n++ {
        ToJSONFile(a, fmt.Sprintf("assignments.%d.json", n))
        ToJSONFile(a.ToLists(), fmt.Sprintf("clusters.%d.json", n))
        i, j, _ := MinLink(m, a)
        a.Merge(i, j)
    }
}

