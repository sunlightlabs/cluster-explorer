
from collections import defaultdict


class Partition(object):
    
    def __init__(self, values):
        self.map = dict([(x, x) for x in values])
        
    def merge(self, x, y):
        x_representative = self.map[x]
        y_representative = self.map[y]
        
        for (key, value) in self.map.items():
            if value == y_representative:
                self.map[key] = x_representative
    
    def sets(self):
        sets = defaultdict(list)
        for (key, value) in self.map.items():
            sets[value].append(key)
            
        return sets.values()