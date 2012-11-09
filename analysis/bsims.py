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

STORED_SIMILARITY_CUTOFFS = [0.9, 0.8, 0.7, 0.6, 0.5]
SIMILARITY_IO_BUFFER_SIZE = 100 * 1024 * 1024 # 100MB, enough so vast majority of corpora fit in single buffer

class SimilarityWriter(object):

	def __init__(self, corpus_id, root=DATA_DIR):
		dir = os.path.join(root, str(corpus_id))
		if not os.path.isdir(dir):
			os.mkdir(dir)
		self.buffers = [list() for _ in range(len(STORED_SIMILARITY_CUTOFFS))]

		self.writers = [BufferedCompressedWriter(open(os.path.join(dir, "%s.sims" % str(9-i)), 'a')) 
						for i in range(len(STORED_SIMILARITY_CUTOFFS))]

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

class LZ4SimilarityWriter(SimilarityWriter):
	def __init__(self, corpus_id, root=DATA_DIR):
		dir = os.path.join(root, str(corpus_id))
		if not os.path.isdir(dir):
			os.mkdir(dir)
		self.buffers = [list() for _ in range(len(STORED_SIMILARITY_CUTOFFS))]

		self.writers = [LZ4CompressedWriter(os.path.join(dir, "%s.lz4sims" % str(9-i))) 
						for i in range(len(STORED_SIMILARITY_CUTOFFS))]

def get_similarity_writer(corpus_id, root=DATA_DIR):
	dir = os.path.join(root, str(corpus_id))
	if os.path.exists(dir) and os.path.exists(os.path.join(dir, "5.lz4sims")):
		return LZ4SimilarityWriter(corpus_id, root)
	else:
		return SimilarityWriter(corpus_id, root)

class SimilarityReader(object):

	def __init__(self, corpus_id, root=DATA_DIR):
		self.dir = os.path.join(root, str(corpus_id))

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

def get_similarity_reader(corpus_id, root=DATA_DIR):
	dir = os.path.join(root, str(corpus_id))
	if os.path.exists(dir) and os.path.exists(os.path.join(dir, "5.lz4sims")):
		print "using lz4"
		return LZ4SimilarityReader(corpus_id, root)
	else:
		return SimilarityReader(corpus_id, root)

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

def convert_to_lz4(corpus_id, preserve_zlib=False):
	"""Convert a zlib corpus to an LZ4 corpus."""
	existing_dir = os.path.join(DATA_DIR, str(corpus_id))
	if not os.path.isdir(existing_dir):
		# only n-gram parsed corpora have similarity data. If no directory, then skip.
		print "skipping, couldn't find %s" % existing_dir
		return

	with LZ4SimilarityWriter(corpus_id) as w:
		i = 0
		for (x, y, s) in SimilarityReader(corpus_id):
			w.write(x, y, s)
			
			i += 1
			if i % 10000000 == 0:
				w.flush()
				sys.stdout.write('.')
				sys.stdout.flush()

	if not preserve_zlib:
		for sims_file in [fname for fname in os.listdir(existing_dir) if fname.endswith(".sims")]:
			os.unlink(sims_file)

def remove_all(corpus_id):
	"""Completely remove similarity directory for corpus."""
	existing_dir = os.path.join(DATA_DIR, str(corpus_id))
	if os.path.isdir(existing_dir):
		shutil.rmtree(existing_dir)

def exists(corpus_id):
	return os.path.isdir(os.path.join(DATA_DIR, str(corpus_id)))


