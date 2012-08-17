try:
	import numpypy
except:
	pass
import numpy
from cStringIO import StringIO
from datetime import datetime

from django.db import connection

from utils import profile

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

	print "Executed query at %s" % datetime.now()

	i = 0
	xs = numpy.empty(cursor.rowcount, numpy.uint32)
	ys = numpy.empty(cursor.rowcount, numpy.uint32)
	sims = numpy.empty(cursor.rowcount, numpy.float32)
	for (x, y, s) in cursor.fetchall():
		xs[i], ys[i], sims[i] = x, y, s
		i += 1

	print "Fetched results at %s" % datetime.now()

	serialization = '\\x' + (xs.tostring() + ys.tostring() + sims.tostring()).encode('hex')

	print "Appended strings at %s" % datetime.now()

	cursor.execute("""
		insert into similarities_binary
		values (%s, %s)
	""", [corpus_id, serialization])

	print "Inserted string at %s" % datetime.now()



@profile
def deserialize_similarities(corpus_id):
	cursor = connection.cursor()

	buffer = StringIO()

	print "Starting copy query at %s" % datetime.now()

	cursor.copy_expert("""
		copy 
			(select serialization from similarities_binary where corpus_id = %s) 
		to STDOUT csv
	""" % int(corpus_id), buffer)

	print "Finished copy query at %s" % datetime.now()

	# strip leading '\x' and trailing newline and decode
	decoded = buffer.getvalue()[2:-1].decode('hex')

	print "Decoded at %s" % datetime.now()

	if len(decoded) % 3 != 0:
		raise "Decoded string had length %s. Length should be divisible by 3." % len(decoded)

	xs = numpy.fromstring(decoded[:len(decoded) / 3], numpy.uint32)
	ys = numpy.fromstring(decoded[len(decoded) / 3: 2 * len(decoded) / 3], numpy.uint32)
	sims = numpy.fromstring(decoded[2 * len(decoded) / 3:], numpy.float32)
	
	print "numpy processing done at %s" % datetime.now()

	# conversion back to Python ints makes follow up
	# computations much faster in PyPy
	xs = [int(x) for x in xs]
	ys = [int(y) for y in ys]
	sims = [float(s) for s in sims]

	print "Python conversion done at %s" % datetime.now()

	return (xs, ys, sims)



