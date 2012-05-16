
from collections import defaultdict


class Partition(object):
    
    def __init__(self, values):
        # these two don't change after initialization
        self.values = list(values)
        self.value_positions = dict(zip(values, range(len(values))))
        
        # this map is updated by merge()
        self.map = range(len(values))
        
    def merge(self, x, y):
        x_representative = self.map[self.value_positions[x]]
        y_representative = self.map[self.value_positions[y]]

        if x_representative != y_representative:
            for i in xrange(len(self.map)):
                if self.map[i] == y_representative:
                    self.map[i] = x_representative
    
    def sets(self):
        sets = defaultdict(list)
        for i in xrange(len(self.map)):
            sets[self.map[i]].append(self.values[i])
            
        return sets.values()