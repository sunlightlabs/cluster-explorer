
from django.db import connection

from parser import break_sentences

from phrases import PhraseSequencer


class DocumentIngester(object):
    
    def __init__(self):
        self.data = list()
        self.max_doc_id = 0 # this should be set by querying documents table
    
    def record(self, text, phrases, metadata):
        # this could record to a file rather than memory
        # some or all of metadata may be explicit parameters
        
        max_doc_id += 1
        data.append(max_doc_id, text, phrases, metadata)
        
    def upload_new_documents(self):
        """Upload document text and phrase occurrences
        
        Return list of new document_ids
        
        """
        pass


def parse(text, sequencer):
    phrases = [sequencer.sequence(sentence) for sentence in break_sentences(text)]
    phrases.sort()
    return phrases


def ingest_documents(docs):
    """Ingest set of new documents"""
    
    sequencer = PhraseSequencer(None) # need cursor here
    ingester = DocumentIngester()
    
    for doc in docs:
        phrases = parse(document, sequencer)
        ingester.record(doc.text, phrases, doc.metadata)

    # in a transaction
    sequencer.upload_new_phrases()
    new_doc_ids = ingester.upload_new_documents()
    
    return new_doc_ids
    

def compute_similarities(new_doc_ids):
    docs = load_all_documents()
    
    for (x, y) in pairs_for_comparison(new_doc_ids):
        similarity = similarity(docs[x], docs[y])
        record(x, y, similarity)
        
    upload_similarities(record)

        
    
    
        
