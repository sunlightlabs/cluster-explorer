
import os
import json
import sys
import subprocess

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


if __name__ == '__main__':
    sourcefile = sys.argv[1]
    root = os.path.dirname(sourcefile)

    ngrams = NGramSpace(4)

    print "parsing documents at %s..." % root
    docs = []
    for row in json.load(open(sourcefile, 'r')):
        docs += [LSDocument(row.get('name_of_filer', ''), row['date_posted'], doc['text'], ngrams) for doc in row['documents'] if doc.get('text')]

    print "computing similarities for %d documents..." % len(docs)
    clustering = Clustering([doc.parsed for doc in docs])

    print "writing similarities to disk..."
    clustering.distance.write_binary(os.path.join(root, 'out', 'docs.sim'))
    json.dump([d.to_dict() for d in docs], open(os.path.join(root, 'out', 'docs.json'), 'w'))
    json.dump(len(docs), open(os.path.join(root, 'out', 'num_steps.json'), 'w'))
    
    print "running clustering..."
    subprocess.call(['go/main', os.path.join(root, 'out')])
    
