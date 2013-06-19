try:
	import numpypy
except:
	pass
import numpy
import os
import sys
import tempfile
from itertools import chain
import shutil

from django.conf import settings

from utils import BufferedCompressedWriter, BufferedCompressedReader, LZ4CompressedWriter, LZ4CompressedReader


DATA_DIR = getattr(settings, 'SIMS_DATA_DIR', '.')

TYPE_PREFERENCE = ('lz4', 'zlib', 'lsm')
TYPE_EXTENSIONS = {'lz4': 'lz4sims', 'zlib': 'sims', 'lsm': 'lsmsims'}

STORED_SIMILARITY_CUTOFFS = [0.9, 0.8, 0.7, 0.6, 0.5]
SIMILARITY_IO_BUFFER_SIZE = 100 * 1024 * 1024 # 100MB, enough so vast majority of corpora fit in single buffer

class SimilarityWriter(object):
	def __init__(self, corpus_id, root=DATA_DIR):
		self.directory = os.path.join(root, str(corpus_id))
		if not os.path.isdir(self.directory):
			os.mkdir(self.directory)
		self.buffers = [list() for _ in range(len(STORED_SIMILARITY_CUTOFFS))]

		@property
		def writers(self):
			raise NotImplementedError

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_value, traceback):
		self.close()

	def write(self, x, y, s):
		i = 0
		while STORED_SIMILARITY_CUTOFFS[i] > s:
			i += 1
			if i == len(STORED_SIMILARITY_CUTOFFS):
				return

		self.buffers[i] += (x, y)

	def flush(self):
		for i in range(len(STORED_SIMILARITY_CUTOFFS)):
			serialization = numpy.array(self.buffers[i], numpy.uint32).tostring()
			self.buffers[i] = []
			self.writers[i].write(serialization)
			self.writers[i].flush()

	def close(self):
		self.flush()
		for w in self.writers:
			w.close()

class ZlibSimilarityWriter(SimilarityWriter):
	def __init__(self, corpus_id, root=DATA_DIR):
		super(ZlibSimilarityWriter, self).__init__(corpus_id, root)
		self.writers = [BufferedCompressedWriter(open(os.path.join(self.directory, "%s.sims" % str(9-i)), 'a')) 
						for i in range(len(STORED_SIMILARITY_CUTOFFS))]

class LZ4SimilarityWriter(SimilarityWriter):
	def __init__(self, corpus_id, root=DATA_DIR):
		super(LZ4SimilarityWriter, self).__init__(corpus_id, root)
		self.writers = [LZ4CompressedWriter(os.path.join(self.directory, "%s.lz4sims" % str(9-i)))
						for i in range(len(STORED_SIMILARITY_CUTOFFS))]

TYPE_WRITERS = {'zlib': ZlibSimilarityWriter, 'lz4': LZ4SimilarityWriter}

def get_similarity_data_type(corpus_id, root=DATA_DIR):
	dir = os.path.join(root, str(corpus_id))
	data_type = None

	# if any of the data files exist, in order of preference, use those
	if os.path.exists(dir):
		for dtype in TYPE_PREFERENCE:
			if os.path.exists(os.path.join(dir, "5.%s" % TYPE_EXTENSIONS[dtype])):
				data_type = dtype
				break
	# if we still don't know, use the default
	if data_type is None:
		data_type = TYPE_PREFERENCE[0]

	return data_type

def get_similarity_writer(corpus_id, root=DATA_DIR, force_data_type=None):
	data_type = force_data_type if force_data_type else get_similarity_data_type(corpus_id, root)
	print "writer using %s" % data_type
	return TYPE_WRITERS[data_type](corpus_id, root)

class SimilarityReader(object):
	def __init__(self, corpus_id, root=DATA_DIR):
		self.dir = os.path.join(root, str(corpus_id))

class ZlibSimilarityReader(SimilarityReader):
	def __iter__(self):
		return chain.from_iterable(
			(self._file_iter(os.path.join(self.dir, "%s.sims" % str(9-i)), STORED_SIMILARITY_CUTOFFS[i] + 0.05) 
			for i in range(len(STORED_SIMILARITY_CUTOFFS))))

	def _file_iter(self, filename, similarity):
		with BufferedCompressedReader(open(filename, 'r')) as reader:

			serialized_bytes = reader.read(SIMILARITY_IO_BUFFER_SIZE)
			while serialized_bytes:
				pairs = numpy.fromstring(serialized_bytes, numpy.uint32)
				for i in range(0, len(pairs), 2):
					yield (int(pairs[i]), int(pairs[i+1]), similarity)

				serialized_bytes = reader.read(SIMILARITY_IO_BUFFER_SIZE)

class LZ4SimilarityReader(SimilarityReader):
	def __iter__(self):
		return chain.from_iterable(
			(self._file_iter(os.path.join(self.dir, "%s.lz4sims" % str(9-i)), STORED_SIMILARITY_CUTOFFS[i] + 0.05) 
			for i in range(len(STORED_SIMILARITY_CUTOFFS))))

	def _file_iter(self, filename, similarity):
		with LZ4CompressedReader(filename) as reader:

			serialized_bytes = reader.read(SIMILARITY_IO_BUFFER_SIZE)
			while serialized_bytes:
				pairs = numpy.fromstring(serialized_bytes, numpy.uint32)
				for i in range(0, len(pairs), 2):
					yield (int(pairs[i]), int(pairs[i+1]), similarity)

				serialized_bytes = reader.read(SIMILARITY_IO_BUFFER_SIZE)

	def files_by_cutoff(self):
		return (
			(
				STORED_SIMILARITY_CUTOFFS[i],
				[
					os.path.join(self.dir, "%s.lz4sims" % str(9-i), f)	
					for f in sorted(os.listdir(os.path.join(self.dir, "%s.lz4sims" % str(9-i))), key=lambda x: int(x.split('.')[0]))
				],
			)
			for i in range(len(STORED_SIMILARITY_CUTOFFS))
		)

TYPE_READERS = {'zlib': ZlibSimilarityReader, 'lz4': LZ4SimilarityReader}

def get_similarity_reader(corpus_id, root=DATA_DIR, force_data_type=None):
	data_type = force_data_type if force_data_type else get_similarity_data_type(corpus_id, root)
	print "reader using %s" % data_type
	return TYPE_READERS[data_type](corpus_id, root)

def remove_documents(corpus_id, doc_ids):
	"""Remove any similarity containing the given doc_ids."""
	existing_dir = os.path.join(DATA_DIR, str(corpus_id))
	if not os.path.isdir(existing_dir):
		# only n-gram parsed corpora have similarity data. If no directory, then skip.
		print "skipping, couldn't find %s" % existing_dir
		return

	temp_dir = tempfile.mkdtemp()

	deletion_set = set(doc_ids) # set could be faster than large list
	reader = get_similarity_reader(corpus_id)
	Writer = LZ4SimilarityWriter if type(reader) == LZ4SimilarityReader else SimilarityWriter
	with Writer(corpus_id, temp_dir) as w:
		i = 0
		for (x, y, s) in reader:
			if x not in deletion_set and y not in deletion_set:
				w.write(x, y, s)
				
				i += 1
				if i % 10000000 == 0:
					w.flush()
					sys.stdout.write('.')
					sys.stdout.flush()

	shutil.rmtree(existing_dir)
	shutil.move(os.path.join(temp_dir, str(corpus_id)), existing_dir)
	shutil.rmtree(temp_dir)

def convert_data_format(corpus_id, preserve_src=True, dest_data_dir=DATA_DIR, src_data_format="zlib", dest_data_format="lz4"):
	"""Convert a zlib corpus to an LZ4 corpus."""
	existing_dir = os.path.join(DATA_DIR, str(corpus_id))
	if not os.path.isdir(existing_dir):
		# only n-gram parsed corpora have similarity data. If no directory, then skip.
		print "skipping, couldn't find %s" % existing_dir
		return

	with TYPE_WRITERS[dest_data_format](corpus_id, root=dest_data_dir) as w:
		i = 0
		for (x, y, s) in TYPE_READERS[src_data_format](corpus_id):
			w.write(x, y, s)
			
			i += 1
			if i % 10000000 == 0:
				w.flush()
				sys.stdout.write('.')
				sys.stdout.flush()

	if not preserve_src:
		for sims_file in [fname for fname in os.listdir(existing_dir) if fname.endswith(".%s" % TYPE_EXTENSIONS[src_data_format])]:
			os.unlink(sims_file)

def bulk_convert(corpus_ids, dest_data_dir, src_data_format="zlib", dest_data_format="lz4"):
	"""Convert a bunch of corpora from one format to another, in a separate process to prevent memory runaway."""
	import multiprocessing
	for corpus_id in corpus_ids:
		print "Converting corpus %s from %s to %s..." % (corpus_id, src_data_format, dest_data_format)
		p = multiprocessing.Process(target=convert_to_lz4, args=[corpus_id, True, dest_data_dir, DATA_DIR, src_data_format, dest_data_format])
		p.start()
		p.join()

def remove_all(corpus_id):
	"""Completely remove similarity directory for corpus."""
	existing_dir = os.path.join(DATA_DIR, str(corpus_id))
	if os.path.isdir(existing_dir):
		shutil.rmtree(existing_dir)

def exists(corpus_id):
	return os.path.isdir(os.path.join(DATA_DIR, str(corpus_id)))


