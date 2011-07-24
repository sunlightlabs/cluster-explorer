
package main


type assignment []int

type Assignment interface {
    IsSameCluster(i, j int) bool
    Merge(from, to int)
}

func NewAssignment(size int) Assignment {
    data := make([]int, size)
    for i := range data {
        data[i] = i
    }
    return assignment(data)
}

func (assignments assignment) Merge(from, to int) {
    rep_from := assignments[from]
    rep_to := assignments[to]
    
    for i := range assignments {
        if assignments[i] == rep_from {
            assignments[i] = rep_to
        }
    }
}

func (assignments assignment) IsSameCluster(i, j int) bool {
    return assignments[i] == assignments[j]
}

func MinLink(distances TriangleMatrix, assignments Assignment) (min_i, min_j int, found bool) {
    var min_v float32
    size := distances.Size()
    
    for i := 0; i < size; i++ {
        for j := 0; j < i; j++ {
            v := distances.Value(i,j)
            if (!found || v < min_v) && ! assignments.IsSameCluster(i, j) {
                min_i = i
                min_j = j
                min_v = v
                found = true
            } 
        }
    }
    
    return
}




