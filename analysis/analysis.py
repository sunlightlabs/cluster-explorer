
from django.db import connection


def similar_docs(corpus_id, doc_id, min_similarity=0.5):
    """Return list of documents (doc IDs) ranked by similarity to given doc"""
    
    c = connection.cursor()
    c.execute("""
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
    """, dict(corpus_id=corpus_id, doc_id=doc_id, min_similarity=min_similarity))
    
    return c.fetchall()