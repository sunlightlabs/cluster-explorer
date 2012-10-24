from datetime import datetime
import csv
import re
from cStringIO import StringIO
import zlib
import lz4
import os

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


_DEFAULT_BUFFER_SIZE = 100 * 1024 * 1024 # = 100MB

class BufferedCompressedWriter(object):

    def __init__(self, outstream, buffer_size=_DEFAULT_BUFFER_SIZE):
        self.outputstream = outstream
        self.compressor = zlib.compressobj()
        self.buffer_size = buffer_size
        self.buffer = StringIO()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def write(self, bytes):
        self.buffer.write(bytes)

        if self.buffer.tell() >= self.buffer_size:
            self.flush()

    def flush(self):
        buffered_bytes = self.buffer.getvalue()
        self.buffer.truncate(0)

        compressed_bytes = self.compressor.compress(buffered_bytes)

        self.outputstream.write(compressed_bytes)


    def close(self):
        self.flush()
        remaining_compressed_bytes = self.compressor.flush()
        self.outputstream.write(remaining_compressed_bytes)
        self.outputstream.flush()
        self.compressor = None

class LZ4CompressedWriter(BufferedCompressedWriter):
    def __init__(self, outdir, buffer_size=_DEFAULT_BUFFER_SIZE):
        self.outputdir = outdir
        if not os.path.exists(outdir):
            os.mkdir(outdir)

        self.outputstream = open(self._get_next_file(), 'w')
        self.buffer_size = buffer_size
        self.buffer = StringIO()

    def flush(self, keep_closed=False):
        buffered_bytes = self.buffer.getvalue()
        self.buffer.truncate(0)

        if len(buffered_bytes) == 0:
            # we don't want to write an empty file because decompressing it does weird things
            if keep_closed:
                name = self.outputstream.name
                self.outputstream.close()
                os.unlink(name)
                return
            else:
                # if we're still writing, this can be a noop
                return

        compressed_bytes = lz4.compressHC(buffered_bytes)

        self.outputstream.write(compressed_bytes)
        self.outputstream.close()
        if not keep_closed:
            self.outputstream = open(self._get_next_file(), 'w')

    def close(self):
        self.flush(keep_closed=True)

    def _get_next_file(self):
        filenums = [int(fname.split('.')[0]) for fname in os.listdir(self.outputdir) if fname.endswith('.lz4')]
        num = max(filenums) + 1 if filenums else 0
        return os.path.join(self.outputdir, "%s.lz4" % num)


class BufferedCompressedReader(object):

    def __init__(self, inputstream, buffer_size=_DEFAULT_BUFFER_SIZE):
        self.inputstream = inputstream
        self.decompressor = zlib.decompressobj()
        self.compressed_buffer = StringIO()
        self.decompressed_buffer = StringIO()
        self.buffer_size = buffer_size

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        self.decompressor = None
        self.compressed_buffer = None
        self.decompressed_buffer = None

    def read(self, byte_count=None):
        eof = False
        while not eof and self.decompressed_buffer.tell() < byte_count:
            # make sure the number of compressed bytes is at least as large as then number of decompressed bytes we still need
            while not eof and self.compressed_buffer.tell() < byte_count - self.decompressed_buffer.tell():
                new_compressed_bytes = self.inputstream.read(self.buffer_size)
                if not new_compressed_bytes:
                    eof = True
                else:
                    self.compressed_buffer.write(new_compressed_bytes)

            compressed_bytes = self.compressed_buffer.getvalue()
            self.compressed_buffer.truncate(0)
            new_uncompressed_bytes = self.decompressor.decompress(compressed_bytes, byte_count - self.decompressed_buffer.tell())
            self.decompressed_buffer.write(new_uncompressed_bytes)

            if self.decompressor.unconsumed_tail:
                self.compressed_buffer.write(self.decompressor.unconsumed_tail)
            elif self.decompressor.unused_data:
                self.compressed_buffer.write(self.decompressor.unused_data)
                # file may be a sequence of separate compression streams.
                # if we hit unused_data then we're at a new stream and
                # need to reset the decompressor.
                self.decompressor = zlib.decompressobj()

        decompressed = self.decompressed_buffer.getvalue()
        self.decompressed_buffer.close()
        self.decompressed_buffer = StringIO()
        self.decompressed_buffer.write(decompressed[byte_count:])
        return decompressed[:byte_count]

class LZ4CompressedReader(BufferedCompressedReader):
    def __init__(self, indir, buffer_size=_DEFAULT_BUFFER_SIZE):
        self.inputdir = indir
        self.decompressed_buffer = StringIO()
        self.file_iter = self._get_file_iter()

    def close(self):
        self.decompressed_buffer = None

    def read(self, byte_count=None):
        while self.decompressed_buffer.tell() < byte_count:
            try:
                fname = self.file_iter.next()
            except StopIteration:
                break

            compressed = open(fname).read()
            new_uncompressed_bytes = lz4.uncompress(compressed) if compressed else ""
            self.decompressed_buffer.write(new_uncompressed_bytes)

        decompressed = self.decompressed_buffer.getvalue()
        self.decompressed_buffer.close()
        self.decompressed_buffer = StringIO()
        self.decompressed_buffer.write(decompressed[byte_count:])
        return decompressed[:byte_count]

    def _get_file_iter(self):
        filenums = sorted([int(fname.split('.')[0]) for fname in os.listdir(self.inputdir) if fname.endswith('.lz4')])
        files = [os.path.join(self.inputdir, "%s.lz4" % num) for num in filenums]
        return iter(files)


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

