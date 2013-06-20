import os

from .base import FastDBTestCase

from analysis.utils import load_snapshot
from analysis.corpus import Corpus

class SimBackendTestCase(FastDBTestCase):
    def setUp(self):
        super(SimBackendTestCase, self).setUp()

        self.assertTrue('TEST_SIMS' in os.environ)

        self.corpora_info = load_snapshot(os.environ['TEST_SIMS'], sim_root=".")
        self.corpus = Corpus(self.corpora_info['ngram_corpora'][0])

    def test_cluster(self):
        print self.corpus._compute_hierarchy(False)