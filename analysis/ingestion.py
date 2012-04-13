
import tempfile
import csv

from django.db import connection

from parser import break_sentences


class PhraseSequencer(object):

    def __init__(self, corpus_id):
        """Initialize the sequencer from stored phrases"""

        self.corpus_id = corpus_id

        c = connection.cursor()
        
        c.execute("select max(phrase_id) from phrases where corpus_id = %s", [corpus_id])
        result = c.fetchone()
        self.next_id = result[0] + 1 if result[0] is not None else 0
        
        c.execute("select phrase_text, phrase_id from phrases where corpus_id = %s", [corpus_id])
        self.phrase_map = dict(c)
        
        self.new_phrase_file = tempfile.TemporaryFile()
        self.writer = csv.writer(self.new_phrase_file)

    def sequence(self, phrase):
        """Return a unique integer for the phrase
        
        If phrase is new, record for later upload to database.
        
        """

        existing_id = self.phrase_map.get(phrase, None)
        if existing_id is not None:
            return existing_id
            
        self.phrase_map[phrase] = self.next_id
        self.writer.writerow([self.corpus_id, self.next_id, phrase])
        self.next_id += 1
        
        return self.next_id - 1

    def upload_new_phrases(self):
        """Upload phrases created during use of sequencer"""
        
        self.new_phrase_file.flush()
        self.new_phrase_file.seek(0)
        
        connection.cursor().copy_from(self.new_phrase_file, 'phrases', sep=',')
        
        self.new_phrase_file.close()
        self.new_phrase_file = tempfile.TemporaryFile()
        self.writer = csv.writer(self.new_phrase_file)
        
        


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

        
    
    
        
