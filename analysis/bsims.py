try:
	import numpypy
except:
	pass
import numpy
from cStringIO import StringIO
import zlib
import gzip
import struct
import os
from datetime import datetime
import tempfile

from django.db import connection, transaction
from django.core.cache import cache
from django.conf import settings

try:
       from redis import StrictRedis
       redis = StrictRedis(host='localhost', port=6379, db=0)
except:
       pass

from utils import profile


DATA_DIR = getattr(settings, 'SIMS_DATA_DIR', '.')



STORED_SIMILARITY_CUTOFFS = [0.9, 0.8, 0.7, 0.6, 0.5]
SIMILARITY_IO_BUFFER_SIZE = 100 * 1024 * 1024 # 100MB, enough so vast majority of corpora fit in single buffer

class SimilarityWriter(object):

	def __init__(self, corpus_id):
		pass

	def __enter__(self):
		return self

	def __exit__(self):
		self.close()

	def write(x, y, s):
		pass
		# split on similarity
		# write to in-meory buffer
		# when buffer is full, push to compressed temp file

	def close():
		pass
		# flush last buffer
		# move old file to backup, rename temp file, delete backup


class SimilarityReader(object):
	

	def __init__(self):
		pass

	def __iter__(self):
		pass
		# chain iterators on each bucket

	def bucket(cutoff):
		pass
		# open compressed file

		# feed in chunks of compressed file to decompressor

		# yield one at a time



def migrate_similarities():
	cursor = connection.cursor()

	cursor.execute("""
		select distinct corpus_id
		from similarities
		where
			corpus_id not in (select corpus_id from similarities_binary)
	""")

	corpus_ids = list(cursor.fetchall())

	for (corpus_id,) in corpus_ids:
		with transaction.commit_on_success():
			serialize_similarities(corpus_id)


@profile
def deserialize_similarities(corpus_id):
	return numpy_deserialize(pg_get(corpus_id))


@profile
def pg_to_file(corpus_id):
	file_set(corpus_id, decompress(pg_get(corpus_id)))

@profile
def migrate_to_files():
	cursor = connection.cursor()

	cursor.execute("""
		select distinct corpus_id
		from similarities_binary
	""")

	corpus_ids = list(cursor.fetchall())

	for (corpus_id,) in corpus_ids:
		pg_to_file(corpus_id)


@profile
def serialize_similarities(corpus_id):
	cursor = connection.cursor()

	cursor.execute("""
		select count(*)
		from similarities
		where
			corpus_id = %s
	""", [corpus_id])

	rows = int(cursor.fetchone()[0])

	print "Migrating %s similarities from corpus %s at %s..." % (rows, corpus_id, datetime.now())

	with tempfile.NamedTemporaryFile() as tuple_similarities:
		cursor.copy_expert("""
	        COPY (
		        select low_document_id, high_document_id, similarity
		        from similarities
		        where
		            corpus_id = %s
		        order by similarity desc
		    ) to STDOUT
	    """ % corpus_id, tuple_similarities)

		tuple_similarities.seek(0)
		xs = numpy.empty(rows, numpy.uint32)
		ys = numpy.empty(rows, numpy.uint32)
		sims = numpy.empty(rows, numpy.float32)

		i = 0
		for line in tuple_similarities:
			xs[i], ys[i], sims[i] = line.split('\t')
			i += 1

		if i != rows:
			raise Exception("Similarities table had %s entries, but we constructed %s." % (rows, i))

	bytes = compress(xs.tostring() + ys.tostring() + sims.tostring())

	pg_insert(corpus_id, bytes)


@profile
def pg_insert(corpus_id, bytes):
	encoded = '\\x' + bytes.encode('hex')

	cursor = connection.cursor()
	cursor.execute("""
		insert into similarities_binary
		values (%s, %s)
	""", [corpus_id, encoded])

@profile
def pg_update(corpus_id, bytes):
	encoded = '\\x' + bytes.encode('hex')

	cursor = connection.cursor()
	cursor.execute("""
		update similarities_binary
		set serialization = %s
		where
			corpus_id = %s
	""", [encoded, corpus_id])


@profile
def pg_get(corpus_id):
	cursor = connection.cursor()
	buffer = StringIO()

	# note: somewhat clunky copy_expert is used b/c pypy and psycopg2ct
	# together have very slow copying of binary data.
	cursor.copy_expert("""
		copy 
			(select serialization from similarities_binary where corpus_id = %s) 
		to STDOUT csv
	""" % int(corpus_id), buffer)

	# strip leading '\x' and trailing newline and decode
	bytes = buffer.getvalue()[2:-1].decode('hex')

	return bytes


@profile
def file_set(corpus_id, bytes):
	with gzip.open(os.path.join(DATA_DIR, '%s.sim' % corpus_id), 'w') as outfile:
		outfile.write(bytes)

@profile
def file_get(corpus_id):
	if os.path.exists(os.path.join(DATA_DIR, '%s.sim' % corpus_id)):
		with gzip.open(os.path.join(DATA_DIR, '%s.sim' % corpus_id), 'r') as infile:
			return infile.read()
	else:
		return ''

@profile
def cache_upsert(corpus_id, bytes):
	cache.set('analysis.corpus.similarities-%s' % corpus_id, bytes)

@profile
def cache_get(corpus_id):
	return cache.get('analysis.corpus.similarities-%s' % corpus_id)

@profile
def redis_upsert(corpus_id, bytes):
	redis.set('analysis.corpus.similarities-%s' % corpus_id, bytes)

@profile
def redis_get(corpus_id):
	return redis.get('analysis.corpus.similarities-%s' % corpus_id)

@profile
def numpy_serialize(entries):
	xs = numpy.empty(len(entries), numpy.uint32)
	ys = numpy.empty(len(entries), numpy.uint32)
	sims = numpy.empty(len(entries), numpy.float32)

	for i in range(len(entries)):
		xs[i], ys[i], sims[i] = entries[i]

	return xs.tostring() + ys.tostring() + sims.tostring()


@profile
def numpy_deserialize(bytes):
	if len(bytes) % 12 != 0:
		raise Exception("Decoded string had length %s. Length should be divisible by 12 (3 4-byte values per entry)." % len(bytes))

	xs = numpy.fromstring(bytes[:len(bytes) / 3], numpy.uint32)
	ys = numpy.fromstring(bytes[len(bytes) / 3: 2 * len(bytes) / 3], numpy.uint32)
	sims = numpy.fromstring(bytes[2 * len(bytes) / 3:], numpy.float32)
	
	# conversion back to Python ints makes follow up
	# computations much faster in PyPy
	result = list()
	num_entries = len(xs)
	for i in range(num_entries):
		result.append((int(xs[i]), int(ys[i]), float(sims[i])))

	return result

@profile
def struct_deserialize(bytes):
	if len(bytes) % 3 != 0:
		raise "Decoded string had length %s. Length should be divisible by 3." % len(bytes)


	x_bytes = bytes[:len(bytes) / 3]
	y_bytes = bytes[len(bytes) / 3: 2 * len(bytes) / 3]
	sim_bytes = bytes[2 * len(bytes) / 3:]

	xs = struct.unpack('%sI' % (len(x_bytes) / 4), x_bytes)
	ys = struct.unpack('%sI' % (len(y_bytes) / 4), y_bytes)
	sims = struct.unpack('%sf' % (len(sim_bytes) / 4), sim_bytes)

	return (xs, ys, sims)


@profile
def compress(bytes):
	return zlib.compress(bytes, 1)

@profile
def decompress(bytes):
	return zlib.decompress(bytes)


