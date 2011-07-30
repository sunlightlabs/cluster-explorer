
package main

import (
    "fmt"
    "os"
)


func main() {
    sim_file := os.Args[1]
    out_dir := os.Args[2]
    
    fmt.Printf("Loading distance matrix from %s...\n", sim_file)
    
    f, _ := os.Open(sim_file)
    m := ReadTriangleMatrix(f)
    a := NewAssignment(m.Size())
    
    fmt.Printf("Loaded %d documents. Beginning clustering\n", m.Size())
 
    i, j, found := 0, 0, true
    n := 0
    for true {
        ToJSONFile(a.ToLists(), fmt.Sprintf("%s/%d.json", out_dir, n))
        fmt.Printf(".")
        i, j, found = MinLink(m, a)
        if ! found {
            break
        }
        a.Merge(i, j)
        n++
    }
    
    fmt.Printf("\nDone clustering in %d steps.\n", n)
}