
package main

import (
    "io"
    "fmt"
    "math"
    "encoding/binary"
)

type TriangleMatrix interface {
    Value(i, j int) float32
    SetValue(i, j int, v float32)
    Size() int
}


type inlineTriangleMatrix []float32


func NewTriangleMatrix(size int) TriangleMatrix {
    if size < 1 {
        panic(fmt.Sprintf("TriangleMatrix size must be greater than 0: %d", size))
    }
    return inlineTriangleMatrix(make([]float32, size*(size-1)/2))
}

func ReadTriangleMatrix(r io.Reader) TriangleMatrix {
    var size int
    binary.Read(r, binary.LittleEndian, &size)

    var values = make([]float32, size*(size-1)/2)
    binary.Read(r, binary.LittleEndian, values)

    return inlineTriangleMatrix(values)
}

func convert(i, j int) int {
    if i <= j {
        panic(fmt.Sprintf("First argument to TriangleMatrix must be larger than second argument: %d, %d", i, j))
    }
    return i*(i-1)/2 + j
}

func (m inlineTriangleMatrix) Value(i, j int) float32 {
    return m[convert(i, j)]
}

func (m inlineTriangleMatrix) SetValue(i, j int, v float32) {
    m[convert(i, j)] = v
}

func (m inlineTriangleMatrix) Size() int {
    return int(math.Sqrt(float64(2 * len(m)))) + 1
}