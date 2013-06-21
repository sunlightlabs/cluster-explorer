import os, shutil

from .base import FastDBTestCase

from analysis.utils import load_snapshot
from analysis.corpus import Corpus
from analysis.bsims import convert_data_format, get_similarity_reader

class ClusterDataTestCase(FastDBTestCase):
    def setUp(self):
        super(ClusterDataTestCase, self).setUp()

        self.assertTrue('TEST_SIMS' in os.environ)

        self.corpora_info = load_snapshot(os.environ['TEST_SIMS'], sim_root=".")
        self.corpus = Corpus(self.corpora_info['ngram_corpora'][0])

    def tearDown(self):
        super(ClusterDataTestCase, self).tearDown()

        corpus_dir = os.path.join('.', str(self.corpus.id))
        if "KEEP_SIMS" not in os.environ and os.path.exists(corpus_dir):
            print 'Removing tree...'
            shutil.rmtree(corpus_dir)

class BasicClusterDataTestCase(ClusterDataTestCase):
    def test_cluster(self):
        self.corpus._compute_hierarchy(False)

class TypeComparisonTestCase(ClusterDataTestCase):
    def setUp(self):
        super(TypeComparisonTestCase, self).setUp()

        convert_data_format(self.corpus.id, preserve_src=True, src_data_dir=".", dest_data_dir=".", src_data_format="lz4", dest_data_format="zlib")

    def test_same_pairs(self):
        zlib_p = sorted(get_similarity_reader(self.corpus.id, root=".", force_data_type="zlib"))

        lz4_p = sorted(get_similarity_reader(self.corpus.id, root=".", force_data_type="lz4"))

        self.assertEqual(zlib_p, lz4_p)

    def test_same_clustering(self):
        zlib_h = self.corpus._compute_hierarchy(False, force_sim_format="zlib")

        lz4_h = self.corpus._compute_hierarchy(False, force_sim_format="lz4")

        self.assertEqual(zlib_h, lz4_h)