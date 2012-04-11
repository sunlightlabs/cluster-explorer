

class PhraseSequencer(object):

    def __init__(self, cursor):
        """Initialize the sequencer from stored phrases"""
        pass
        
    def sequence(self, phrase):
        """Return a unique integer for the phrase
        
        If phrase is new, record for future upload to database.
        
        """
        pass
        
    def upload_new_phrases(self, cursor):
        """Upload phrases created during use of sequencer"""
        pass
        
class DocumentIngester(object):
    
    def record(self, text, phrases, metadata):
        # this could record to a file rather than memory
        pass
        
    def upload_new_documents(self, cursor):
        """Upload document text and phrase occurrences
        
        Return list of new document_ids
        
        """
        pass



def ingest_documents(docs):
    """Ingest set of new documents
    
    """
    
    sequencer = PhraseSequencer(<db cursor needed>)
    ingester = DocumentIngester()
    
    for doc in docs:
        phrases = parse(document, sequencer)
        record(doc.text, phrases, doc.metadata)

    # in a transaction
    sequencer.upload_new_phrases()
    new_doc_ids = ingester.upload_new_documents()
    
    return new_doc_ids
    

def compute_similarities(new_doc_ids):
    docs = load_all_documents()
    
    for (x, y) in pairs_for_comparison(new_doc_ids):
        similarity = similarity(docs[x], docs[y])
        record (x, y, similarity)
        
    upload_similarities

        
    
    
        
