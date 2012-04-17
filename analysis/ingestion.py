
import tempfile
import csv

from django.db import connection

from parser import break_sentences
from phrases import PhraseSequencer


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
    
    for doc in docs:
        phrases = parse(document, sequencer)
        ingester.record(doc.text, phrases, doc.metadata)

    # in a transaction
    sequencer.upload_new_phrases()
    ingester.upload_new_documents()
    
    return new_doc_ids
    

def compute_similarities(new_doc_ids):
    docs = load_all_documents()
    
    for (x, y) in pairs_for_comparison(new_doc_ids):
        similarity = similarity(docs[x], docs[y])
        record(x, y, similarity)
        
    upload_similarities(record)

        
    
    
        
