
package main

import (
    "io"
    "math"
    "encoding/binary"
)

type SymmetricMatrix interface {
    Value(i, j int) float32
    SetValue(i, j int, v float32)
    Size() int
}


type inlineSymmetricMatrix []float32


func NewSymmetricMatrix(size int) SymmetricMatrix {
    return inlineSymmetricMatrix(make([]float32, size*(size+1)/2))
}

func ReadSymmetricMatrix(r io.Reader) SymmetricMatrix {
    var size int
    binary.Read(r, binary.LittleEndian, &size)

    var values = make([]float32, size*(size+1)/2)
    binary.Read(r, binary.LittleEndian, values)

    return inlineSymmetricMatrix(values)
}

func (m inlineSymmetricMatrix) Value(i, j int) float32 {
    return m[i*(i+1)/2 + j]
}

func (m inlineSymmetricMatrix) SetValue(i, j int, v float32) {
    m[i*(i+1)/2 + j] = v
}

func (m inlineSymmetricMatrix) Size() int {
    return int(math.Sqrt(float64(2 * len(m))))
}