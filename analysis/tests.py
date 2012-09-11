import shutil
import os

from django.test import TestCase
from django.db import connection

from ingestion import *
from phrases import PhraseSequencer
from parser import _sentence_boundaries, _ngram_boundaries, sentence_parse
from utils import execute_file, binary_search
from corpus import Corpus
from partition import Partition
from utils import BufferedCompressedWriter, BufferedCompressedReader
from bsims import SimilarityWriter, SimilarityReader


class DBTestCase(TestCase):
    
    def setUp(self):
        self.cursor = connection.cursor()
        execute_file(self.cursor, os.path.join(os.path.dirname(__file__), 'tables.sql'))
        
        self.corpus = Corpus()
        
    def tearDown(self):
        execute_file(self.cursor, os.path.join(os.path.dirname(__file__), 'drop_tables.sql'))

class TestPartition(TestCase):
    # note: order of returned sets is really undefined.
    # but you can't have sets of sets, so there's no easy
    # way of expressing this in the test. Just changing
    # the expected order to match the observed order on my setup.
    def test_partition(self):
        p = Partition('abcde')
        self.assertEqual([['a'], ['b'], ['c'], ['d'], ['e']], p.sets())
        p.merge('a', 'b')
        self.assertEqual([['a', 'b'], ['c'], ['d'], ['e']], p.sets())
        p.merge('c', 'd')
        self.assertEqual([['a', 'b'], ['c', 'd'], ['e']], p.sets())
        p.merge('a', 'd')
        self.assertEqual([['a', 'c', 'b', 'd'], ['e']], p.sets())
        p.merge('b', 'e')
        self.assertEqual([['a', 'c', 'b', 'e', 'd']], p.sets())
        

class TestSequencer(DBTestCase):
        
    def test_basic(self):
        s = PhraseSequencer(self.corpus)
        a = s.sequence('a')
        b = s.sequence('b')
        c = s.sequence('c')
        
        self.assertEqual(0, a)
        self.assertEqual(1, b)
        self.assertEqual(2, c)
        self.assertEqual(a, s.sequence('a'))
        self.assertEqual(b, s.sequence('b'))
        self.assertEqual(c, s.sequence('c'))
        
    def test_persistence(self):
        s1 = PhraseSequencer(self.corpus)
        a = s1.sequence('a')
        b = s1.sequence('b')
        c = s1.sequence('c')

        # new sequencer shouldn't see updates that haven't been persisted
        # note: should never do this in practice--should only ever be one
        # active sequencer per corpus.
        s2 = PhraseSequencer(self.corpus)
        self.assertEqual(0, s2.sequence('a'))
        
        s1.upload_new_phrases()
        self.assertEqual(1, s1.sequence('b')) # existing phrases still present
        self.assertEqual(3, s1.sequence('d')) # new phrases can still be added
        
        s3 = PhraseSequencer(self.corpus)
        self.assertEqual(2, s3.sequence('c')) # previously uploaded phrase appears
        self.assertEqual(3, s3.sequence('e')) # but not d=3, which wasn't uploaded
        
        s4 = PhraseSequencer(Corpus())
        self.assertEqual(0, s4.sequence('f'))  # sequencer with different corpus doesn't show at all


class TestParser(DBTestCase):
    
    def test_basic(self):
        t1 = "This is a basic text. Two sentences. Maybe three?"
        t2 = "Two sentences. Maybe...three? this is a basic text."
        s = PhraseSequencer(self.corpus)
        
        p1 = sentence_parse(t1, s)
        p2 = sentence_parse(t2, s)
        
        self.assertEqual([(0, [(0, 21)]), (1, [(22, 36)]), (2, [(37, 49)])], p1)
        self.assertEqual([(0, [(30, 51)]), (1, [(0, 14)]), (2, [(15, 29)])], p2)
        
    def test_empty(self):
        s = PhraseSequencer(self.corpus)
        c = connection.cursor()
        
        p = sentence_parse('', s)
        
        self.assertEqual([], p)
        
        c.execute('select count(*) from phrases')
        self.assertEqual(0, c.fetchone()[0])
        
    def test_ngrams(self):        
        self.assertEqual([], _ngram_boundaries('', 3))
        self.assertEqual([], _ngram_boundaries('foobar', 3))
        self.assertEqual([(0,6)], _ngram_boundaries('foobar', 1))
        self.assertEqual([], _ngram_boundaries('foo bar', 3))
        self.assertEqual([(0, 12)], _ngram_boundaries('foo bar spaz', 3))
        
        t1 = "One simple sentence. With punctuation."
        self.assertEqual([(0,19), (4,25), (11, 37)], _ngram_boundaries(t1, 3))
        
        t2 = "One Simple sentence--with punctuation??"
        self.assertEqual([(0,19), (4,25), (11, 37)], _ngram_boundaries(t2, 3))
        
        t3 = "\n testing...trailing spaces.\t "
        self.assertEqual([(2,27)], _ngram_boundaries(t3, 3))
        
    def test_sentence_tokenize(self):
        t = ''
        self.assertEqual([], _sentence_boundaries(t))
        
        t = '   '
        self.assertEqual([], _sentence_boundaries(t))

        t = 'A simple test case. Of two sentences.'
        self.assertEqual([(0, 19), (20, 37)], _sentence_boundaries(t))
        
        t = 'A simple test case. \t \t \n Of two sentences.'
        self.assertEqual([(0, 19), (26, 43)], _sentence_boundaries(t))

    def test_sentence_parse(self):
        s = PhraseSequencer(self.corpus)

        t = ''
        self.assertEqual([], sentence_parse(t, s))

        t = '   '
        self.assertEqual([], sentence_parse(t, s))

        t = 'A simple test case. Of two sentences.'
        self.assertEqual([(0, [(0, 19)]), (1, [(20, 37)])], sentence_parse(t, s))

        t = ' \n A simple test case. \t \t \n Of two sentences.\n'
        self.assertEqual([(0, [(3, 22)]), (1, [(29, 46)])], sentence_parse(t, s))
        
        t = 'of two sentences. of two sentences?'
        self.assertEqual([(1, [(0, 17), (18, 35)])], sentence_parse(t, s))
        
 
class TestDocumentIngester(DBTestCase):
    
    def test_ingester(self):
        i = DocumentIngester(self.corpus)
        s = PhraseSequencer(self.corpus)
        
        t1 = 'This document has three sentences. One of which matches. Two of which do not.'
        t2 = 'This document has only two sentences. One of which matches.'
        
        i._record_document(t1, sentence_parse(t1, s), {})
        i._record_document(t2, sentence_parse(t2, s), {})
        
        s.upload_new_phrases()
        i._upload_new_documents()
        
        c = connection.cursor()
        
        c.execute("select count(*) from documents")
        self.assertEqual(2, c.fetchone()[0])
        
        c.execute("select count(*) from phrase_occurrences")
        self.assertEqual(5, c.fetchone()[0])

        # make sure we can add on to existing data
        i = DocumentIngester(self.corpus)
        s = PhraseSequencer(self.corpus)
        
        t3 = 'This document has only two sentences. Only one of which is new.'
        p3 = sentence_parse(t3, s)
        
        doc_id = i._record_document(t3, p3, {})
        self.assertEqual(2, doc_id)
        self.assertEqual([(3, [(0, 37)]), (4, [(38, 63)])], p3)
        
        s.upload_new_phrases()
        i._upload_new_documents()
        
        c.execute("select count(*) from documents")
        self.assertEqual(3, c.fetchone()[0])
        
        c.execute("select count(*) from phrase_occurrences")
        self.assertEqual(7, c.fetchone()[0])
        
    def test_all_docs(self):
        i = DocumentIngester(self.corpus)

        i.ingest([
            'This document has three sentences. One of which matches. Two of which do not.',
            'This document has only two sentences. One of which matches.',
            'This document has only two sentences. Only one of which is new.',
            ''
        ])
        
        c = connection.cursor()
        c.execute('select count(*) from documents')
        self.assertEqual(4, c.fetchone()[0])
        
        self.assertEqual(dict([(0, [0, 1, 2]), (1, [1, 3]), (2, [3, 4])]), self.corpus.all_docs())

    def test_similarities(self):
        
        self.test_ingester()
        
        i = DocumentIngester(self.corpus)
        i.compute_similarities([0, 1, 2])
        
        c = connection.cursor()
        
        c.execute("select count(*) from similarities")
        self.assertEqual(2, c.fetchone()[0])
        
        self.assertEqual(0.25, self.get_sim(c, 0, 1))
        self.assertAlmostEqual(1.0/3, self.get_sim(c, 1, 2), places=5)

    def test_similarities_cutoff(self):

        self.test_ingester()

        i = DocumentIngester(self.corpus)
        i.compute_similarities([0, 1, 2], min_similarity=0.0)

        c = connection.cursor()

        c.execute("select count(*) from similarities")
        self.assertEqual(3, c.fetchone()[0])

        self.assertEqual(0.25, self.get_sim(c, 0, 1))
        self.assertEqual(0, self.get_sim(c, 0, 2))
        self.assertAlmostEqual(1.0/3, self.get_sim(c, 1, 2), places=5)
        
    def test_complete(self):
        i = DocumentIngester(self.corpus)
        i.ingest([
            'This document has three sentences. One of which matches. Two of which do not.',
            'This document has only two sentences. One of which matches.',
            'This document has only two sentences. Only one of which is new.'
        ])

        c = connection.cursor()
    
        c.execute("select count(*) from similarities")
        self.assertEqual(2, c.fetchone()[0])        
        self.assertEqual(0.25, self.get_sim(c, 0, 1))
        self.assertAlmostEqual(1.0/3, self.get_sim(c, 1, 2), places=5)

        i.ingest([
            "This document matches nothing else.",
            "Only one of which is new."
        ])
 
        c.execute("select count(*) from similarities")
        self.assertEqual(3, c.fetchone()[0])        
        self.assertAlmostEqual(0.5, self.get_sim(c, 2, 4))


    def get_sim(self, c, x, y):
        c.execute("""
            select similarity
            from similarities
            where
                corpus_id = %s
                and low_document_id = %s
                and high_document_id = %s
        """, [self.corpus.id, min(x, y), max(x, y)])
        
        return c.fetchone()[0]
        
class TestAnalysis(DBTestCase):
    
    def test_basic(self):
        i = DocumentIngester(self.corpus)
        i.ingest([
            "This document has three sentences. One of which matches. Two of which do not.",
            "This document has only two sentences. One of which matches.",
            "This document has only two sentences. Only one of which is new.",
            "This document matches nothing else.",
            "Only one of which is new.",
            "There will be two of these.",
            "There will be two of these."
        ])

        self.assertEqual([], self.corpus.similar_docs(3))
        self.assertEqual([], self.corpus.similar_docs(0))
        self.assertEqual([(1, 0.25)], self.corpus.similar_docs(0, min_similarity=0.2))
        self.assertEqual([(5, 1.0)], self.corpus.similar_docs(6))
        self.assertEqual([(6, 1.0)], self.corpus.similar_docs(5))
        self.assertEqual([(4, 0.5)], self.corpus.similar_docs(2))
        sim_2 = self.corpus.similar_docs(2, min_similarity=0.2)
        self.assertEqual(2, len(sim_2))
        self.assertEqual((4, 0.5), sim_2[0])
        self.assertEqual(1, sim_2[1][0])
        self.assertAlmostEqual(1.0/3, sim_2[1][1], places=5)

    def test_phrase_overlap(self):
        i = DocumentIngester(self.corpus)
        i.ingest([
            "This document has three sentences. One of which matches. Two of which do not.",
            "This document has only two sentences. One of which matches.",
            "This document has only two sentences. Only one of which is new.",
            "This document matches nothing else.",
            "Only one of which is new.",
            "There will be two of these.",
            "There will be two of these."
        ])

        overlap = self.corpus.phrase_overlap(2, [id for (id, _) in self.corpus.similar_docs(2, 0.2)])
        self.assertEqual({3: {'count': 1L, 'indexes': '{"(0,37)"}'}, 4: {'count': 1L, 'indexes': '{"(38,63)"}'}}, overlap)
        
        overlap = self.corpus.phrase_overlap(2, [id for (id, _) in self.corpus.similar_docs(2, 0.4)])
        self.assertEqual({4: {'count': 1L, 'indexes': '{"(38,63)"}'}}, overlap)

class TestRealData(DBTestCase):

    def test_multiline_doc(self):
        
        doc = 'First line.\nSecond line.'
        i = DocumentIngester(self.corpus)
        i.ingest([doc])
        
        c = connection.cursor()
        c.execute('select count(*) from documents')
        self.assertEqual(1, c.fetchone()[0])
        

    def test_lightsquared_breakage(self):
        docs = ["Wireless broadband service is important, but it should not come at the expense of GPS.\nAll the studies show that LightSquared?s proposed network would cause interference and that there\nare no remedies.We rely on the FCC to protect the integrity of the GPS signal and we support their\nrecommendation to stop LightSquared?s current proposal. Since LORAN is phased out the\nrecreational Boater needs a reliable source for navigation. Please don't let LightSquared infringe on\nour rights!!!!\nThank You.\nTim\n\n"]

        i = DocumentIngester(self.corpus)
        i.ingest(docs)
    
        c = connection.cursor()
        c.execute('select count(*) from documents')
        self.assertEqual(1, c.fetchone()[0])
        
    def test_duplicated_phrases(self):
        doc = 'The same sentence. The same sentence. A different sentence. The SAME sentence.'

        i = DocumentIngester(self.corpus)
        
        self.assertEqual([(0, [(0, 18), (19, 37), (60, 78)]), (1, [(38, 59)])], sentence_parse(doc, i.sequencer))
        
        i.ingest([doc])

    def test_ingestion_metadata_quoting(self):
        i = DocumentIngester(self.corpus)
        
        doc = dict(text='not important', metadata=dict(title='a "quoted" string'))
        i.ingest([doc])
        
        self.cursor.execute("select metadata -> 'title' from documents")
        self.assertEqual('a "quoted" string', self.cursor.fetchone()[0])

    def test_bad_encoding(self):
        doc = 'This has a bad \xe2 character.'
        metadata = {u'title': "Even the title is \xe2 bad."}
        
        i = DocumentIngester(self.corpus)
        i.ingest([{'text': doc, 'metadata': metadata}])
        
        self.cursor.execute("select metadata -> 'title' from documents")
        self.assertEqual(u'Even the title is \ufffdad.', self.cursor.fetchone()[0])
        
        self.cursor.execute("select text from documents")
        self.assertEqual(u'This has a bad \ufffdharacter.', self.cursor.fetchone()[0])


class TestBinarySearch(TestCase):
    
    def test(self):
        self.assertEqual(0, binary_search([], 9))
        self.assertEqual(0, binary_search([1], 2))
        self.assertEqual(1, binary_search([1], 1))
        self.assertEqual(0, binary_search([2, 1], 3))
        self.assertEqual(1, binary_search([2, 1], 2))
        self.assertEqual(1, binary_search([2, 1], 1.5))
        self.assertEqual(2, binary_search([2, 1], 1))
        self.assertEqual(2, binary_search([2, 1], 0))
        self.assertEqual(0, binary_search([3, 2, 1], 4))
        self.assertEqual(1, binary_search([3, 2, 1], 3))
        self.assertEqual(2, binary_search([3, 2, 1], 2))
        self.assertEqual(3, binary_search([3, 2, 1], 0))
        self.assertEqual(0, binary_search([6, 5, 4, 3, 2, 1], 7))
        self.assertEqual(4, binary_search([6, 5, 4, 3, 2, 1], 3))
        self.assertEqual(6, binary_search([6, 5, 4, 3, 2, 1], 0))
        self.assertEqual(0, binary_search([7, 6, 5, 4, 3, 2, 1], 8))
        self.assertEqual(1, binary_search([7, 6, 5, 4, 3, 2, 1], 7))
        self.assertEqual(5, binary_search([7, 6, 5, 4, 3, 2, 1], 3))
        self.assertEqual(7, binary_search([7, 6, 5, 4, 3, 2, 1], 0))
        
        self.assertEqual(4, binary_search([2, 2, 2, 2, 1], 2))
        self.assertEqual(5, binary_search([2, 2, 2, 2, 1], 1))
        

class TestBufferedCompressedIO(TestCase):

    def assertReadWriterConsistent(self, value, buffer_size=1000000):
        with BufferedCompressedWriter(open('test.out', 'w'), buffer_size) as w:
            w.write(value)

        with BufferedCompressedReader(open('test.out', 'r'), buffer_size) as r:
            self.assertEqual(value, r.read(len(value)))

    def test(self):
        testfile = 'test.out'
        try:
            self.assertReadWriterConsistent('')
            self.assertReadWriterConsistent('a')
            self.assertReadWriterConsistent('apples')
            self.assertReadWriterConsistent("".join([chr(i) for i in range(256)]))

            self.assertReadWriterConsistent('', 1)
            self.assertReadWriterConsistent('a', 1)
            self.assertReadWriterConsistent('apples', 1)
            self.assertReadWriterConsistent("".join([chr(i) for i in range(256)]), 1)

            self.assertReadWriterConsistent('', 1)
            self.assertReadWriterConsistent('a', 1)
            self.assertReadWriterConsistent('apples', 5)
            self.assertReadWriterConsistent('apples', 6)
            self.assertReadWriterConsistent('apples', 7)
        finally:
            os.remove(testfile)

    def test_append(self):
        testfile = 'test.out'
        try:
            with BufferedCompressedWriter(open(testfile, 'w')) as w:
                w.write('three ')
            with BufferedCompressedWriter(open(testfile, 'a')) as w:
                w.write('separate ')
            with BufferedCompressedWriter(open(testfile, 'a')) as w:
                w.write('writes')

            with BufferedCompressedReader(open('test.out', 'r')) as r:
                self.assertEqual('three separate writes', r.read(len('three separate writes')))

            with BufferedCompressedReader(open('test.out', 'r'), 2) as r:
                self.assertEqual('three separate writes', r.read(len('three separate writes')))

            with BufferedCompressedReader(open('test.out', 'r')) as r:
                self.assertEqual('three separate writes', r.read(len('three ')) + r.read(len('sepa')) + r.read(len('rate writes')))

        finally:
            os.remove(testfile)

    def test_sim_io(self):
        shutil.rmtree(os.path.join('.', '0'))

        with SimilarityWriter(0, root='.') as w:
            w.write(1,2,1.0)
            w.write(1,3,0.95)
            w.write(1,4,0.9)
            w.write(2,5,0.8)
            w.write(2,6,0.72)
            w.write(2,7,0.7)
            w.write(3,8,0.5)
            w.write(3,9,0.4)

        r = SimilarityReader(0, root='.')
        values = list(r)
        self.assertEqual([(1,2,.9 + .05),
                          (1,3,.9 + .05),
                          (1,4,.9 + .05),
                          (2,5,.8 + .05),
                          (2,6,.7 + .05),
                          (2,7,.7 + .05),
                          (3,8,.5 + .05)],
                        values)


if __name__ == '__main__':
    unittest.main()
