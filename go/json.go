
package main


import (
    "os"
    "json"
)


func ToJSONFile(v interface{}, filename string) os.Error {
    outfile, error := os.Create(filename)
    if error != nil {
        return error
    }
    
    encoder := json.NewEncoder(outfile)
    error = encoder.Encode(v)
    if error != nil {
        return error
    }
    
    outfile.Close()
    return nil
}