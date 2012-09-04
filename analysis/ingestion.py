
import tempfile
import csv
import sys

from parser import sentence_parse
from phrases import PhraseSequencer
from utils import jaccard, UnicodeWriter
from bsims import SimilarityWriter

class DocumentIngester(object):
    
    def __init__(self, corpus, parser=sentence_parse, compute_similarities=True):
        """Return a new ingester for the corpus.
        
        parser may be sentence_parse or ngram_parser(n)
        
        Client must insure that no other ingester is running
        concurrently on the same corpus.
        """
        
        self.corpus = corpus
        self.parser = parser
        self.compute_similarities = compute_similarities
        
        max_doc_id = corpus.max_doc_id()
        self.next_id = max_doc_id + 1 if max_doc_id is not None else 0
        
        self.document_file = tempfile.TemporaryFile()
        self.document_writer = UnicodeWriter(self.document_file)

        self.occurrence_file = tempfile.TemporaryFile()
        
        self.sequencer = PhraseSequencer(corpus)
        
    
    def _record_document(self, text, phrases, metadata):
        doc_id = self.next_id
        self.next_id += 1
        
        formatted_metadata = ",".join([('"%s"=>"%s"' % (key, value.replace('\\', '\\\\').replace('"', '\\"'))) for (key, value) in metadata.items()])
        self.document_writer.writerow([str(self.corpus.id), str(doc_id), text, formatted_metadata])
        
        for (phrase_id, indexes) in phrases:
            formatted_indexes = '"{%s}"' % ", ".join(['""(%s, %s)""' % (start, end) for (start, end) in indexes])
            self.occurrence_file.write("%s,%s,%s,%s\n" % (self.corpus.id, doc_id, phrase_id, formatted_indexes))

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


    def ingest(self, docs):
        """Ingest set of new documents"""
        
        new_doc_ids = list()
        
        print "parsing %s documents..." % len(docs)
    
        for doc in docs:
            if isinstance(doc, basestring):
                text = doc
                metadata = {}
            else:
                text = doc['text']
                metadata = doc['metadata']

            phrases = self.parser.__call__(text, self.sequencer)
            id = self._record_document(text, phrases, metadata)
            new_doc_ids.append(id)
            
        print "uploading documents..."
        
        self.sequencer.upload_new_phrases()
        self._upload_new_documents()
        
        if self.compute_similarities:
            print "computing similarities..."
            self._compute_similarities(new_doc_ids)

    @staticmethod
    def _pairs_for_comparison(all_ids, new_ids):
        allowed_ids = set(all_ids)
        all_ids = list(all_ids)
        all_ids.sort()
    
        new_ids = list(new_ids)
        new_ids.sort(reverse=True)
    
        for x in all_ids:
            for y in new_ids:
                if x >= y:
                    break
                if y in allowed_ids:
                    yield (x, y)

    def _compute_similarities(self, new_doc_ids, min_similarity=0.5):
        docs = self.corpus.all_docs()
    
        with SimilarityWriter(self.corpus.id) as writer:
            i = 0
            for (x, y) in self._pairs_for_comparison(docs.keys(), new_doc_ids):
                similarity = jaccard(docs[x], docs[y])
                if similarity >= min_similarity:
                    writer.write(x, y, similarity)
                
                i += 1
                if i % 10000000 == 0:
                    writer.flush()
                    sys.stdout.write('.')
                    sys.stdout.flush()

