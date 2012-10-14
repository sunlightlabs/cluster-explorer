import cffi, os, platform
from collections import defaultdict
from partition import Partition
try:
    import numpypy as numpy
except ImportError:
    import numpy

libtype = "dylib" if platform.uname()[0] == "Darwin" else "so"

ffi = cffi.FFI()

ffi.cdef("void* create_cpartition(int* values, int length);")
ffi.cdef("void free_cpartition(void* part);")
ffi.cdef("int cpartition_find(void* part, int x);")
ffi.cdef("int cpartition_find_by_value(void* part, int x);")
ffi.cdef("void cpartition_merge(void* part, int x, int y);")

class cPartition(Partition):
    def __init__(self, values):
        self.values = numpy.array(values, dtype='i')
        self.part = ffi.create_cpartition(self.values, self.values.size)

    def _find(self, x):
        return ffi.cpartition_find(self.part, x)

    def _merge(self, x, y):
        ffi.cpartition_merge(self.part, x, y)

    def sets(self):
        sets = defaultdict(list)
        for position in xrange(self.values.size):
            sets[ffi.cpartition_find(self.part, x)] = self.values[position]

        return sets.values()

    def sets_overview(self):
        sets = defaultdict(int)
        for position in xrange(self.values.size):
            sets[ffi.cpartition_find(self.part, x)] += 1

        return sets

    def group(self, x):
        result = set()
        xRoot = ffi.cpartition_find_by_value(self.part, x)
        for position in xrange(self.values.size):
            if ffi.cpartition_find(self.part, position) == xRoot:
                result.add(self.values[position])

        return result

    def representative(self, x):
        return self.values[ffi.cpartition_find_by_value(self.part, x)]