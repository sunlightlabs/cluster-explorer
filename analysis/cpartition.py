import cffi, os, platform
from collections import defaultdict
from partition import Partition
import array

libtype = "dylib" if platform.uname()[0] == "Darwin" else "so"

ffi = cffi.FFI()

ffi.cdef("void* create_cpartition(int* values, int length);")
ffi.cdef("void free_cpartition(void* part);")
ffi.cdef("int cpartition_find(void* part, int x);")
ffi.cdef("int cpartition_find_by_value(void* part, int x);")
ffi.cdef("void cpartition_merge(void* part, int x, int y);")
libcpartition = ffi.dlopen(os.path.join(os.path.dirname(os.path.abspath(__file__)), "libcpartition.%s" % libtype))

class cPartition(Partition):
    def __init__(self, values):
        self.count = len(values)
        _values = ffi.new("int[]", self.count)
        for k, v in enumerate(values):
            _values[k] = v
        self.values = _values
        self.part = libcpartition.create_cpartition(_values, self.count)

    def __del__(self):
        if self.part is not None:
            libcpartition.free_cpartition(self.part)

    def _find(self, x):
        return libcpartition.cpartition_find(self.part, x)

    def merge(self, x, y):
        libcpartition.cpartition_merge(self.part, x, y)

    def sets(self):
        sets = defaultdict(list)
        for position in xrange(self.count):
            sets[libcpartition.cpartition_find(self.part, position)].append(self.values[position])

        return sets.values()

    def sets_overview(self):
        sets = defaultdict(int)
        for position in xrange(self.count):
            sets[libcpartition.cpartition_find(self.part, position)] += 1

        return sets

    def group(self, x):
        result = set()
        xRoot = libcpartition.cpartition_find_by_value(self.part, x)
        for position in xrange(self.count):
            if libcpartition.cpartition_find(self.part, position) == xRoot:
                result.add(self.values[position])

        return result

    def representative(self, x):
        return self.values[libcpartition.cpartition_find_by_value(self.part, x)]

    def free(self):
        if self.part is not None:
            libcpartition.free_cpartition(self.part)
            self.part = None
        self.values = None