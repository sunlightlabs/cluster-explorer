
import cPickle
import csv
import sys

from interactive import dump_to_csv
from cftc import CFTCDocument


def singleton_count(cluster_map):
    c = 0
    for value in cluster_map.values:
        if len(value) == 1:
            c += 1
    
    return c

    
if __name__ == '__main__':
    in_file = sys.argv[1]
    out_file = sys.argv[2]
    print "Reading existing clustering from %s..." % in_file 
    (clustering, docs) = cPickle.load(open(in_file, 'rb'))
    
    i = 0
    while True:
        (x, y) = clustering.min_link()
        if x is None or y is None:
            break
        
        clustering.merge(x, y)
        
        dump_to_csv(clustering, docs, "%s.%d.csv" % (out_file, i))
        
        i += 1
        
        
        
