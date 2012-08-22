try:
	import numpypy
except:
	pass
import numpy
from cStringIO import StringIO
import zlib
import struct

from django.db import connection
from django.core.cache import cache

from redis import StrictRedis
redis = StrictRedis(host='localhost', port=6379, db=0)

from utils import profile


@profile
def deserialize_similarities(corpus_id):
	return numpy_deserialize(pg_get(corpus_id))


@profile
def serialize_similarities(corpus_id):
	cursor = connection.cursor()

	cursor.execute("""
        select low_document_id, high_document_id, similarity
        from similarities
        where
            corpus_id = %s
        order by similarity desc
    """, [corpus_id])

	bytes = numpy_serialize(cursor.fetchall())

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
	if len(bytes) % 3 != 0:
		raise "Decoded string had length %s. Length should be divisible by 3." % len(bytes)

	xs = numpy.fromstring(bytes[:len(bytes) / 3], numpy.uint32)
	ys = numpy.fromstring(bytes[len(bytes) / 3: 2 * len(bytes) / 3], numpy.uint32)
	sims = numpy.fromstring(bytes[2 * len(bytes) / 3:], numpy.float32)
	
	# conversion back to Python ints makes follow up
	# computations much faster in PyPy
	xs = [int(x) for x in xs]
	ys = [int(y) for y in ys]
	sims = [float(s) for s in sims]

	return (xs, ys, sims)

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


