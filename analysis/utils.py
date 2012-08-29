from datetime import datetime
import csv
import re

from django.conf import settings

def execute_file(cursor, filename):
    contents = " ".join([line for line in open(filename, 'r') if line[0:2] != '--'])
    statements = contents.split(';')[:-1] # split on semi-colon. Last element will be trailing whitespace

    for statement in statements:
        cursor.execute(statement)

class UnicodeWriter:
    """A wrapper around csv.writer that properly handles unicode.

    string inputs are assumed to be utf8 encoded. Output is utf8.
    Python 2.x allows surrogate code points, which are not legal unicode
    and will be rejected by postgres. See:
    http://en.wikipedia.org/wiki/UTF-8#Invalid_code_points
    http://bugs.python.org/issue8271#msg102209
    Surrogates are replaced with the unicode replacement 
    character.
    """

    def __init__(self, f):
        self.writer = csv.writer(f)

    def writerow(self, row):
        self.writer.writerow([self._encode(s) for s in row])

    @staticmethod
    def _encode(value):
        if isinstance(value, unicode):
            u = value
        elif isinstance(value, str):
            u = unicode(value, 'utf8', 'replace')
        else:
            u = unicode(value)
        surrogates_removed = re.sub(ur'[\ud800-\udfff]', u'\uFFFD', u)
        return surrogates_removed.encode('utf8', 'replace')


def binary_search(a, x, key=None):
    """Given a sorted (decreasing) list, return the first element that is less than the target value."""
    
    left = 0
    right = len(a)
    
    while (left < right):
        mid = (right + left) / 2
        
        if (key(a[mid]) if key else a[mid]) >= x:
            left = mid + 1
        else:
            right = mid
    
    return right

def profile(f):    
    def profiled_f(*args, **opts):
        f_name = "%s.%s.%s" % (f.__module__, args[0].__class__.__name__ if args else None, f.__name__)
        print "Entering %s" % f_name
        start = datetime.now()
        result = f(*args, **opts)
        print "Exiting %s after %s" % (f_name, datetime.now() - start)
        return result
    
    if getattr(settings, 'ANALYSIS_PROFILE', False):
        return profiled_f
    
    return f


def overlap(x, y):
    """Return the size of the intersection of two sorted arrays."""
    
    i = 0
    j = 0

    c = 0

    len_x = len(x)
    len_y = len(y)

    while i < len_x and j < len_y:
        if x[i] > y[j]:
            j += 1
        elif x[i] < y[j]:
            i += 1
        else: # x[i] == y[j]
            c += 1
            i += 1
            j += 1

    return c


def jaccard(x, y):
    """Return the jaccard measure of two sets, represented as sorted arrays of integers."""
    
    intersection_size = overlap(x, y)
    union_size = len(x) + len(y) - intersection_size

    return float(intersection_size) / union_size if union_size != 0 else 0

