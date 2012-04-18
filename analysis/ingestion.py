
import tempfile
import csv

from django.db import connection

from parser import parse
from phrases import PhraseSequencer
from cluster.ngrams import jaccard


class DocumentIngester(object):
    
    def __init__(self, corpus_id):
        self.corpus_id = corpus_id

        c = connection.cursor()
        
        c.execute("select max(document_id) from documents where corpus_id = %s", [corpus_id])
        result = c.fetchone()
        self.next_id = result[0] + 1 if result[0] is not None else 0
        
        self.document_file = tempfile.TemporaryFile()
        self.document_writer = csv.writer(self.document_file)

        self.occurrence_file = tempfile.TemporaryFile()
        self.occurrence_writer = csv.writer(self.occurrence_file)
        
    
    def record(self, text, phrases, metadata=None):
        # some or all of metadata may be explicit parameters
        doc_id = self.next_id
        self.next_id += 1
        
        self.document_writer.writerow([self.corpus_id, doc_id, text])
        
        for phrase_id in phrases:
            self.occurrence_writer.writerow([self.corpus_id, doc_id, phrase_id])

        return doc_id 
        
        
    def upload_new_documents(self):
        """Upload document text and phrase occurrences
        
        Return list of new document_ids
        
        """
        
        self.document_file.flush()
        self.document_file.seek(0)
        connection.cursor().copy_from(self.document_file, 'documents', sep=',')
        self.document_file.close()
        self.document_file = tempfile.TemporaryFile()
        self.document_writer = csv.writer(self.document_file)

        self.occurrence_file.flush()
        self.occurrence_file.seek(0)
        connection.cursor().copy_from(self.occurrence_file, 'phrase_occurrences', sep=',')
        self.occurrence_file.close()
        self.occurrence_file = tempfile.TemporaryFile()
        self.occurrence_writer = csv.writer(self.occurrence_file)


def ingest_documents(corpus_id, docs):
    """Ingest set of new documents"""
    
    sequencer = PhraseSequencer(corpus_id)
    ingester = DocumentIngester(corpus_id)
    
    new_doc_ids = list()
    
    for doc in docs:
        phrases = parse(doc, sequencer)
        id = ingester.record(doc, phrases)
        new_doc_ids.append(id)
        
    sequencer.upload_new_phrases()
    ingester.upload_new_documents()
    
    compute_similarities(corpus_id, new_doc_ids)


def pairs_for_comparison(all_ids, new_ids):
    all_ids = list(all_ids)
    all_ids.sort()
    
    new_ids = list(new_ids)
    new_ids.sort(reverse=True)
    
    for x in all_ids:
        for y in new_ids:
            if x >= y:
                break
            yield (x, y)                

docs_stmt = """
    select document_id, array_agg(phrase_id)
    from (
        select document_id, phrase_id
        from phrase_occurrences
        where
            corpus_id = %s
        order by document_id, phrase_id) x
    group by document_id
"""

def compute_similarities(corpus_id, new_doc_ids):
    sim_file = tempfile.TemporaryFile()
    sim_writer = csv.writer(sim_file)
    
    c = connection.cursor()
    
    c.execute(docs_stmt, [corpus_id])
    docs = dict(c.fetchall())
    
    for (x, y) in pairs_for_comparison(docs.keys(), new_doc_ids):
        similarity = jaccard(docs[x], docs[y])
        sim_writer.writerow([corpus_id, x, y, similarity])
        
    sim_file.flush()
    sim_file.seek(0)
    c.copy_from(sim_file, 'similarities', sep=",")
    sim_file.close()

        
    
    
        
