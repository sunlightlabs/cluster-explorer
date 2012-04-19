
import tempfile
import csv

from parser import parse
from phrases import PhraseSequencer
from cluster.ngrams import jaccard
from corpus import Corpus


class DocumentIngester(object):
    
    def __init__(self, corpus):
        self.corpus = corpus
        
        max_doc_id = corpus.get_max_doc_id()
        self.next_id = max_doc_id + 1 if max_doc_id is not None else 0
        
        self.document_file = tempfile.TemporaryFile()
        self.document_writer = csv.writer(self.document_file)

        self.occurrence_file = tempfile.TemporaryFile()
        self.occurrence_writer = csv.writer(self.occurrence_file)
        
        self.sequencer = PhraseSequencer(corpus)
        
    
    def _record_document(self, text, phrases, metadata=None):
        # some or all of metadata may be explicit parameters
        doc_id = self.next_id
        self.next_id += 1
        
        self.document_writer.writerow([self.corpus.id, doc_id, text])
        
        for phrase_id in phrases:
            self.occurrence_writer.writerow([self.corpus.id, doc_id, phrase_id])

        return doc_id 
        
        
    def _upload_new_documents(self):
        """Upload document text and phrase occurrences
        
        Return list of new document_ids
        
        """
        
        self.document_file.flush()
        self.document_file.seek(0)
        self.corpus.upload_csv(self.document_file, 'documents')
        self.document_file.close()
        self.document_file = tempfile.TemporaryFile()
        self.document_writer = csv.writer(self.document_file)

        self.occurrence_file.flush()
        self.occurrence_file.seek(0)
        self.corpus.upload_csv(self.occurrence_file, 'phrase_occurrences')
        self.occurrence_file.close()
        self.occurrence_file = tempfile.TemporaryFile()
        self.occurrence_writer = csv.writer(self.occurrence_file)


    def ingest(self, docs):
        """Ingest set of new documents"""
        
        new_doc_ids = list()
        
        print "parsing documents..."
    
        for doc in docs:
            phrases = parse(doc, self.sequencer)
            id = self._record_document(doc, phrases)
            new_doc_ids.append(id)
            
        print "uploading documents..."
        
        self.sequencer.upload_new_phrases()
        self._upload_new_documents()
        
        print "computing similarities..."
    
        self._compute_similarities(new_doc_ids)

    @staticmethod
    def _pairs_for_comparison(all_ids, new_ids):
        all_ids = list(all_ids)
        all_ids.sort()
    
        new_ids = list(new_ids)
        new_ids.sort(reverse=True)
    
        for x in all_ids:
            for y in new_ids:
                if x >= y:
                    break
                yield (x, y)                

    def _compute_similarities(self, new_doc_ids, min_similarity=0.1):
        sim_file = tempfile.TemporaryFile()
        sim_writer = csv.writer(sim_file)
        
        docs = self.corpus.get_all_docs()
    
        i = 0
    
        for (x, y) in self._pairs_for_comparison(docs.keys(), new_doc_ids):
            similarity = jaccard(docs[x], docs[y])
            if similarity >= min_similarity:
                sim_writer.writerow([self.corpus.id, x, y, similarity])
            
            i += 1
            if i % 1000 == 0:
                sys.stdout.write('.')
        
        sim_file.flush()
        sim_file.seek(0)
        self.corpus.upload_csv(sim_file, 'similarities')
        sim_file.close()

        
    
    
        
