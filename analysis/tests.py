import os

from django.test import TestCase
from django.db import connection, transaction

from ingestion import *
from phrases import PhraseSequencer
from parser import sentence_parse, ngram_parse, sentence_boundaries, sentence_indexed_parse
from sql_utils import execute_file
from corpus import Corpus


class DBTestCase(TestCase):
    
    def setUp(self):
        self.cursor = connection.cursor()
        execute_file(self.cursor, os.path.join(os.path.dirname(__file__), 'tables.sql'))
        
        self.corpus = Corpus()
        
    def tearDown(self):
        execute_file(self.cursor, os.path.join(os.path.dirname(__file__), 'drop_tables.sql'))
        

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
        
        self.assertEqual([0, 1, 2], p1)
        self.assertEqual([0, 1, 2], p2)
        
    def test_empty(self):
        s = PhraseSequencer(self.corpus)
        c = connection.cursor()
        
        p = sentence_parse('', s)
        
        self.assertEqual([], p)
        
        c.execute('select count(*) from phrases')
        self.assertEqual(0, c.fetchone()[0])
        
    def test_ngrams(self):
        s = PhraseSequencer(self.corpus)
        t1 = "One simple sentence. With punctuation."
        t2 = "One Simple sentence--with punctuation?"
        
        p1_1 = ngram_parse(t1, 1, s)
        p2_1 = ngram_parse(t2, 1, s)
        
        self.assertEqual([0, 1, 2, 3, 4], p1_1)
        self.assertEqual(p1_1, p2_1)
        
        p1_3 = ngram_parse(t1, 3, s)
        p2_3 = ngram_parse(t2, 3, s)
        
        self.assertEqual([5, 6, 7], p1_3)
        self.assertEqual(p1_3, p2_3)
        
        p1_5 = ngram_parse(t1, 5, s)
        p2_5 = ngram_parse(t2, 5, s)
        
        self.assertEqual([8], p1_5)
        self.assertEqual(p1_5, p2_5)
        
        p1_6 = ngram_parse(t1, 6, s)
        p2_6 = ngram_parse(t2, 6, s)
        
        self.assertEqual([], p1_6)
        self.assertEqual(p1_6, p2_6)
        
class TestIndexingParser(DBTestCase):
    
    def test_sentence_tokenize(self):
        t = ''
        self.assertEqual([], sentence_boundaries(t))
        
        t = '   '
        self.assertEqual([], sentence_boundaries(t))

        t = 'A simple test case. Of two sentences.'
        self.assertEqual([(0, 19), (20, 37)], sentence_boundaries(t))
        
        t = 'A simple test case. \t \t \n Of two sentences.'
        self.assertEqual([(0, 19), (26, 43)], sentence_boundaries(t))

    def test_sentence_indexed_parse(self):
        s = PhraseSequencer(self.corpus)

        t = ''
        self.assertEqual([], sentence_indexed_parse(t, s))

        t = '   '
        self.assertEqual([], sentence_indexed_parse(t, s))

        t = 'A simple test case. Of two sentences.'
        self.assertEqual([(0, [(0, 19)]), (1, [(20, 37)])], sentence_indexed_parse(t, s))

        t = ' \n A simple test case. \t \t \n Of two sentences.\n'
        self.assertEqual([(0, [(3, 22)]), (1, [(29, 46)])], sentence_indexed_parse(t, s))
        
        t = 'of two sentences. of two sentences?'
        self.assertEqual([(1, [(0, 17), (18, 35)])], sentence_indexed_parse(t, s))
        
 
class TestDocumentIngester(DBTestCase):
    
    def test_ingester(self):
        i = DocumentIngester(self.corpus)
        s = PhraseSequencer(self.corpus)
        
        t1 = 'This document has three sentences. One of which matches. Two of which do not.'
        t2 = 'This document has only two sentences. One of which matches.'
        
        i._record_document(t1, sentence_indexed_parse(t1, s), {})
        i._record_document(t2, sentence_indexed_parse(t2, s), {})
        
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
        p3 = sentence_indexed_parse(t3, s)
        
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
        s = PhraseSequencer(self.corpus)

        i.ingest([
            'This document has three sentences. One of which matches. Two of which do not.',
            'This document has only two sentences. One of which matches.',
            'This document has only two sentences. Only one of which is new.',
            ''
        ])
        
        c = connection.cursor()
        c.execute('select count(*) from documents')
        self.assertEqual(4, c.fetchone()[0])
        
        self.assertEqual(dict([(0, [0, 1, 2]), (1, [1, 3]), (2, [3, 4]), [3, []]]), self.corpus.all_docs())

    def test_similarities(self):
        
        self.test_ingester()
        
        i = DocumentIngester(self.corpus)
        i._compute_similarities([0, 1, 2])
        
        c = connection.cursor()
        
        c.execute("select count(*) from similarities")
        self.assertEqual(2, c.fetchone()[0])
        
        self.assertEqual(0.25, self.get_sim(c, 0, 1))
        self.assertAlmostEqual(1.0/3, self.get_sim(c, 1, 2), places=5)

    def test_similarities_cutoff(self):

        self.test_ingester()

        i = DocumentIngester(self.corpus)
        i._compute_similarities([0, 1, 2], min_similarity=0.0)

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

        overlap = self.corpus.phrase_overlap(2, 0.2)
        self.assertEqual({3: 1, 4:1}, overlap)
        
        overlap = self.corpus.phrase_overlap(2, 0.4)
        self.assertEqual({4:1}, overlap)

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
        
        self.assertEqual([(0, [(0, 18), (19, 37), (60, 78)]), (1, [(38, 59)])], sentence_indexed_parse(doc, i.sequencer))
        
        i.ingest([doc])

    def test_ingestion_metadata_quoting(self):
        i = DocumentIngester(self.corpus)
        
        doc = dict(text='not important', metadata=dict(title='a "quoted" string'))
        i.ingest([doc])
        
        self.cursor.execute("select metadata -> 'title' from documents")
        self.assertEqual('a "quoted" string', self.cursor.fetchone()[0])

    def test_bad_encoding(self):
        doc = 'This has a bad \xe2 character.'
        metadata = {'title': "Even the title is \xe2 bad."}
        
        i = DocumentIngester(self.corpus)
        i.ingest([{'text': doc, 'metadata': metadata}])
        
        self.cursor.execute("select metadata -> 'title' from documents")
        self.assertEqual(u'Even the title is \ufffdad.', self.cursor.fetchone()[0])
        
        self.cursor.execute("select text from documents")
        self.assertEqual(u'This has a bad \ufffdharacter.', self.cursor.fetchone()[0])


if __name__ == '__main__':
    unittest.main()
