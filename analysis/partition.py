
from collections import defaultdict


class Partition(object):
    """Implements an efficient union/find algorithm.
    
    See http://en.wikipedia.org/wiki/Disjoint-set_data_structure
    """
    
    def __init__(self, values):
        # these two don't change after initialization
        self.values = list(values)
        self.value_positions = dict(zip(values, range(len(values))))
        
        # these are updated by merge and find
        self.parent = range(len(values))
        self.rank = [0] * len(values)

    # operates on positions in the arrays, NOT on
    # values as passed to __init__().
    def _find(self, x):
        if self.parent[x] != x:
            self.parent[x] = self._find(self.parent[x])
        return self.parent[x]
        
    def merge(self, x, y):
        xRoot = self._find(self.value_positions[x])
        yRoot = self._find(self.value_positions[y])
        if xRoot == yRoot:
            return
        
        if self.rank[xRoot] < self.rank[yRoot]:
            self.parent[xRoot] = yRoot
        elif self.rank[xRoot] > self.rank[yRoot]:
            self.parent[yRoot] = xRoot
        else:
            self.parent[yRoot] = xRoot
            self.rank[xRoot] += 1
    
    def sets(self):
        sets = defaultdict(list)
        for (value, position) in self.value_positions.iteritems():
            sets[self._find(position)].append(value)
       
        return sets.values()

    def group(self, x):
        """Return set of all items grouped with x."""
        
        result = set()
        xRoot = self._find(self.value_positions[x])
        for (value, position) in self.value_positions.iteritems():
            if self._find(position) == xRoot:
                result.add(value)
        
        return result
        