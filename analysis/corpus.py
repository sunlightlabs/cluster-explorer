
from django.db import connection


class Corpus(object):
    
    def __init__(self, corpus_id=None):
        self.cursor = connection.cursor()

        if corpus_id is None:
            self.cursor.execute("insert into corpora default values returning corpus_id")
            self.id = self.cursor.fetchone()[0]
        else:
            self.id = corpus_id
        
    def get_max_doc_id(self):
        self.cursor.execute("select max(document_id) from documents where corpus_id = %s", [self.id])
        result = self.cursor.fetchone()
        
        return result[0] if result else None
        
    def get_max_phrase_id(self):
        self.cursor.execute("select max(phrase_id) from phrases where corpus_id = %s", [self.id])
        result = self.cursor.fetchone()
        
        return result[0] if result else None
        
    def upload_csv(self, file, tablename):
        self.cursor.copy_from(file, tablename, sep=',')
    
    def get_all_docs(self):
        self.cursor.execute("""
            select document_id, array_agg(phrase_id)
            from (
                select document_id, phrase_id
                from phrase_occurrences
                where
                    corpus_id = %s
                order by document_id, phrase_id) x
            group by document_id
        """, [self.id])
        
        return dict(self.cursor.fetchall())
    
    def get_all_phrases(self):
        self.cursor.execute("select phrase_text, phrase_id from phrases where corpus_id = %s", [self.id])
        return dict(self.cursor)

