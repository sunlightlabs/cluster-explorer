
import os
import cPickle
import json
import sys

from clustering import Clustering
from ngrams import NGramSpace 



class LSDocument(object):
    
    def __init__(self, name, date, text, ngrams):
        self.name = name
        self.date = date
        self.text = text
        self.parsed = ngrams.parse(self.text)
        
    def __str__(self):
        return "%s (%d)\n%s" % (self.name, self.date, self.text)

    @classmethod
    def get_output_headers(self):
        return ['name', 'date', 'text']
    
    def get_output_values(self):
        return [self.name, self.date, self.text]
    
    def to_dict(self):
        return dict(name=self.name, date=self.date, text=self.text)
        

def setup(source):
    ngrams = NGramSpace(4)
    print "parsing documents at %s..." % source
    docs = []
    for row in json.load(open(source, 'r')):
        docs += [LSDocument(row.get('name_of_filer', ''), row['date_posted'], doc['text'], ngrams) for doc in row['documents'] if doc.get('text')]
    print "clustering %d documents..." % len(docs)
    clustering = Clustering([doc.parsed for doc in docs])
    return (clustering, docs)





if __name__ == '__main__':
    root = os.path.dirname(sys.argv[1])
    (clustering, docs) = setup(sys.argv[1])
    print "\nWriting clustering to %s..." % os.path.join(root, 'clustering.pickle')
    cPickle.dump((clustering, docs), open(os.path.join(root, 'clustering.pickle'), 'wb'), cPickle.HIGHEST_PROTOCOL)
    clustering.distance.write_binary(os.path.join(root, 'ls.sim'))
    json.dump([d.to_dict() for d in docs], open(os.path.join(root, 'out', 'docs.json'), 'w'))
    
