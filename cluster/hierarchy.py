import cPickle
import csv
import sys
import json
import StringIO

from cluster.cftc import CFTCDocument
from cluster.clustering import Clustering
from cluster.interactive import dump_to_csv


class IndirectList(object):
    
    def __init__(self, indexes, values):
        self.indexes = indexes
        self.values = values
        
    def __len__(self):
        return len(self.indexes)
    
    def __getitem__(self, i):
        if isinstance(i, int):
            return self.values[self.indexes[i]]
        if isinstance(i, slice):
            return IndirectList(self.indexes.__getitem__(i), self.values)
        
        raise TypeError, "__getitem__ expects either int or slice, not %s" % type(i)        
        
    def __iter__(self):
        for i in xrange(len(self)):
            yield self[i]


class ClusterHierarchy(object):
    
    def __init__(self, data_path):
        self.data_path = data_path
        self.docs = json.load(open("%s/docs.json" % data_path, 'r'))
        self.num_steps = int(json.load(open("%s/num_steps.json" % data_path, 'r')))
    
    def __len__(self):
        return self.num_steps
        
    def __getitem__(self, n):
        clusters = json.load(open("%s/%d.json" % (self.data_path, n), 'r'))
        return [IndirectList(cluster, self.docs) for cluster in clusters]

    def stream_csv(self, step):
        csvbuffer = StringIO.StringIO()
        
        def to_ascii(data):
            if data:
                return unicode(data).encode('ascii', 'replace')
            else:
                return ''
        
        writer = csv.writer(csvbuffer)
        writer.writerow(['cluster number','name', 'org', 'date', 'text'])
        
        clusters = self[step]
        
        for i in range(0, len(clusters)):
            for d in clusters[len(clusters)-i-1]:
                """
                uses len(clusters)-i-1 in array index but writes len(clusters)-1 to file.
                this is so the CSV will match the web frontend, which is 1-indexed.
                """
                writer.writerow([len(clusters)-i] + [to_ascii(x) for x in (d['name'], d.get('org', ''), d['date'], d['text'])])
                csvbuffer.seek(0)
                data = csvbuffer.read()
                csvbuffer.seek(0)
                csvbuffer.truncate()
                yield data
        csvbuffer.close()


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
        sys.stdout.flush()
        i += 1
    
    print "\n Writing result of %d steps..." % i    
    cPickle.dump((docs, clusterings), open(out_file, 'wb'), cPickle.HIGHEST_PROTOCOL)
