
from django.db import connection

from partition import Partition


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
        self.cursor.copy_expert("copy %s from STDIN csv" % tablename, file)
    
    def get_all_docs(self):
        self.cursor.execute("""
            select document_id, coalesce(phrases, ARRAY[]::integer[])
            from documents
            left join (
                select document_id, array_agg(phrase_id order by phrase_id) as phrases
                from phrase_occurrences
                where
                    corpus_id = %s
                group by document_id) x using (document_id)
        """, [self.id])
        
        return dict(self.cursor.fetchall())
    
    def get_all_phrases(self):
        self.cursor.execute("select phrase_text, phrase_id from phrases where corpus_id = %s", [self.id])
        return dict(self.cursor)
        
    def get_common_phrases(self, limit=10):
        self.cursor.execute("""
            select phrase_id, phrase_text, count(distinct document_id)
            from phrase_occurrences
            inner join phrases using (corpus_id, phrase_id)
            where
                corpus_id = %s
            group by phrase_id, phrase_text
            order by count(distinct document_id) desc
            limit %s
        """, [self.id, limit])
        
        return self.cursor.fetchall()

    def get_docs_containing_phrase(self, phrase_id):
        self.cursor.execute("""
            select document_id, substring(text for 1000)
            from documents
            inner join phrase_occurrences using (corpus_id, document_id)
            where
                corpus_id = %s
                and phrase_id = %s
            group by document_id, substring(text for 1000)
            order by document_id
        """, [self.id, phrase_id])
        
        return self.cursor.fetchall()
        
    def get_centroid_doc(self, doc_ids):
        # SQL doesn't support empty lists with IN operator, so check here to avoid SQL error
        if not doc_ids:
            return None
            
        self.cursor.execute("""
            with included_sims as (
                select low_document_id, high_document_id, similarity
                from similarities
                where
                    corpus_id = %(corpus_id)s
                    and low_document_id in %(doc_ids)s
                    and high_document_id in %(doc_ids)s
            )
            select document_id, text
            from (
                select low_document_id as document_id, similarity from included_sims
                union all
                select high_document_id, similarity from included_sims
            ) x
            inner join documents using (document_id)
            where
                documents.corpus_id = %(corpus_id)s
            group by document_id, text
            order by sum(similarity) desc
            limit 1
        """, dict(corpus_id=self.id, doc_ids=tuple(doc_ids)))
        
        return self.cursor.fetchone()

    def get_clusters(self, min_similarity):
        self.cursor.execute("""
                select low_document_id, high_document_id
                from similarities
                where
                    corpus_id = %(corpus_id)s
                    and similarity >= %(min_similarity)s
        """, dict(corpus_id=self.id, min_similarity=min_similarity))

        edges = self.cursor.fetchall()
        vertices = set([x for (x, y) in edges] + [y for (x, y) in edges])
        
        partition = Partition(vertices)
        
        for (x, y) in edges:
            partition.merge(x, y)
            
        return partition.sets()

    # TODO: shouldn't this return results sorted by similarity?
    def similar_docs(self, doc_id, min_similarity=0.5):
        """Return list of documents (doc IDs) ranked by similarity to given doc"""

        self.cursor.execute("""
            select doc_id, similarity
            from (
                select high_document_id as doc_id, similarity
                from similarities
                where
                    corpus_id = %(corpus_id)s
                    and low_document_id = %(doc_id)s
                    and similarity >= %(min_similarity)s
            union all
                select low_document_id as doc_id, similarity
                from similarities
                where
                    corpus_id = %(corpus_id)s
                    and high_document_id = %(doc_id)s
                    and similarity >= %(min_similarity)s
            ) x
            order by similarity desc
        """, dict(corpus_id=self.id, doc_id=doc_id, min_similarity=min_similarity))

        return self.cursor.fetchall()
        
    def phrase_overlap(self, doc_id, min_similarity):
        self.cursor.execute("""
            with
                similar_documents as (
                        select high_document_id as doc_id, similarity
                        from similarities
                        where
                            corpus_id = %(corpus_id)s
                            and low_document_id = %(doc_id)s
                            and similarity >= %(min_similarity)s
                    union all
                        select low_document_id as doc_id, similarity
                        from similarities
                        where
                            corpus_id = %(corpus_id)s
                            and high_document_id = %(doc_id)s
                            and similarity >= %(min_similarity)s
                ),
                phrases_in_target_doc as (
                    select phrase_id
                    from phrase_occurrences
                    where
                        corpus_id = %(corpus_id)s
                        and document_id = %(doc_id)s
                )
            select phrase_id, count(*)
            from phrases_in_target_doc
            left join phrase_occurrences using (phrase_id)
            where
                corpus_id = %(corpus_id)s
                and document_id in (select doc_id from similar_documents)
            group by phrase_id
        """, dict(corpus_id=self.id, doc_id=doc_id, min_similarity=min_similarity))
        
        return dict(self.cursor)
