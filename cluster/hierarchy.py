from cftc import CFTCDocument
from clustering import Clustering
from interactive import dump_to_csv
import cPickle
import csv
import sys


class IndirectList(object):
    
    def __init__(self, indexes, values):
        self.indexes = indexes
        self.values = values
        
    def __len__(self):
        return len(self.indexes)
    
    def __getitem__(self, i):
        return self.values[self.indexes[i]]
    
    def __iter__(self):
        for i in xrange(len(self)):
            yield self[i]


def load_hierarchy(path):
    (clustering, docs, steps) = cPickle.load(open(path, 'rb'))
    
    return [[IndirectList(cluster, docs) for cluster in clustering] for clustering in steps]
    

if __name__ == '__main__':
    in_file = sys.argv[1]
    out_file = sys.argv[2]
    print "Reading existing clustering from %s..." % in_file 
    (clustering, docs) = cPickle.load(open(in_file, 'rb'))
    
    clusterings = list()
    
    i = 0
    while True:
        (x, y) = clustering.min_link()
        if x is None or y is None:
            break
        
        clustering.merge(x, y)
        
        clusters = [c for c in clustering.get_clusters().values() if len(c) > 1]
        clusters.sort(key=len, reverse=True)        
        
        clusterings.append(clusters)
        #dump_to_csv(clustering, docs, "%s.%d.csv" % (out_file, i))
        
        sys.stdout.write('.')
        i += 1
    
    print "\n Writing result of %d steps..." % i    
    cPickle.dump((clustering, docs, clusterings), open(out_file, 'wb'), cPickle.HIGHEST_PROTOCOL)