
import psycopg2.extras
from django.db import connection
from django.core.cache import cache

from partition import Partition
from cluster.ngrams import jaccard

# Django connection is a wrappers around psycopg2 connection,
# but that wrapped object isn't initialized till a call is made.
connection.cursor()
psycopg2.extras.register_composite('int_bounds', connection.connection)
psycopg2.extras.register_hstore(connection.connection)


def get_corpora_by_metadata(key, value):
    """Return a list of Corpus objects having the given key and value.
    
    For example assuming the corpora were built with 'agency' and 'docket' keys,
    passing 'agency' and 'OSHA' would return all OSHA coprora. Passing 'docket' and
    a docket ID would return a list of a single corpus.
    """

    c = connection.cursor()
    c.execute("select corpus_id from corpora where metadata -> %s = %s", [key, value])
    return [Corpus(id) for (id,) in c.fetchall()]


class Corpus(object):
    
    def __init__(self, corpus_id=None, metadata={}):
        """Return wrapper to all database access on the corpus.
        
        If no corpus_id given then new empty corpus created.
        """
        
        self.cursor = connection.cursor()

        if corpus_id is None:
            self.cursor.execute("insert into corpora (metadata) values (%s) returning corpus_id", [metadata])
            self.id = self.cursor.fetchone()[0]
            self.metadata = metadata
        else:
            self.id = corpus_id
            self.cursor.execute("select metadata from corpora where corpus_id = %s", [corpus_id])
            self.metadata = self.cursor.fetchone()[0]


    ### methods used by DocumentIngester ###    
        
    def max_doc_id(self):
        self.cursor.execute("select max(document_id) from documents where corpus_id = %s", [self.id])
        result = self.cursor.fetchone()
        
        return result[0] if result else None
        
    def max_phrase_id(self):
        self.cursor.execute("select max(phrase_id) from phrases where corpus_id = %s", [self.id])
        result = self.cursor.fetchone()
        
        return result[0] if result else None
        
    def upload_csv(self, file, tablename):
        self.cursor.copy_expert("copy %s from STDIN csv" % tablename, file)
    
    def all_docs(self):
        """Return the phrases in all non-empty documents."""
        
        self.cursor.execute("""
            select document_id, array_agg(phrase_id order by phrase_id) as phrases
            from phrase_occurrences
            where
                corpus_id = %(corpus_id)s
            group by document_id
        """, dict(corpus_id=self.id))
        
        return dict(self.cursor.fetchall())
    
    def all_phrases(self):
        self.cursor.execute("select phrase_text, phrase_id from phrases where corpus_id = %s", [self.id])
        return dict(self.cursor)
    
    
    ### methods used by clients ###
    
    def doc(self, doc_id):
        """Return the text and metadata of a given document."""
        
        self.cursor.execute("""
            select text, metadata
            from documents
            where
                corpus_id = %s
                and document_id = %s
        """, [self.id, doc_id])
        
        (text, metadata) = self.cursor.fetchone()
        return dict(text=text, metadata=metadata)
        
    def docs_by_metadata(self, key, value):
        """Return IDs of all documents matching the given metadata key/value."""
        
        self.cursor.execute("""
            select document_id
            from documents
            where
                corpus_id = %s
                and metadata -> %s = %s
        """, [self.id, key, value])

        return [id for (id,) in self.cursor.fetchall()]
    
    def common_phrases(self, limit=10):
        """Return the most frequent phrases in corpus.
        
        Result is a list of (phrase ID, phrase text, count),
        sorted by count.
        """
        
        self.cursor.execute("""
            select phrase_id as found_phrase, count,
                (select substring(text for (indexes[1].end - indexes[1].start) from indexes[1].start + 1)
                from phrase_occurrences i
                inner join documents using (corpus_id, document_id)
                where
                    corpus_id = %(corpus_id)s
                    and x.phrase_id = i.phrase_id
                limit 1) as example_phrase
            from (
                select phrase_id, count(distinct document_id)
                from phrase_occurrences o
                inner join phrases using (corpus_id, phrase_id)
                where
                    corpus_id = %(corpus_id)s
                group by phrase_id, phrase_text
                order by count(distinct document_id) desc
                limit %(limit)s) x;
        """, dict(corpus_id=self.id, limit=limit))
        
        return self.cursor.fetchall()

    def docs_containing_phrase(self, phrase_id):
        """Return all documents containing the given phrase.
        
        Result is a list of (document ID, (start offset, end offset), document text preview),
        sorted by document ID.
        """
        
        self.cursor.execute("""
            select document_id, indexes, substring(text for (indexes[1].end - indexes[1].start) from indexes[1].start + 1) as sample
            from documents
            inner join phrase_occurrences using (corpus_id, document_id)
            where
                corpus_id = %s
                and phrase_id = %s
            group by document_id, indexes, sample
            order by document_id
        """, [self.id, phrase_id])
        
        return self.cursor.fetchall()
        
    def docs_by_centrality(self, doc_ids):
        """Return the document from given document set with minimum average
        distance to other documents in the set.
        
        Document set may be any arbitrary collection of IDs from the corpus.
        
        Result is (document ID, document text).
        """
        
        # SQL doesn't support empty lists with IN operator, so check here to avoid SQL error
        if not doc_ids:
            return None
            
        self.cursor.execute("""
            with included_sims as (
                select unnest(ARRAY[low_document_id, high_document_id]) as document_id, similarity
                from similarities
                where
                    corpus_id = %(corpus_id)s
                    and low_document_id in %(doc_ids)s
                    and high_document_id in %(doc_ids)s
            )
            select document_id, metadata, sum(similarity)::float / %(num_docs)s
            from included_sims
            inner join documents using (document_id)
            where
                documents.corpus_id = %(corpus_id)s
            group by document_id, metadata
            order by sum(similarity) desc
        """, dict(corpus_id=self.id, doc_ids=tuple(doc_ids), num_docs=len(doc_ids)))

        return self.cursor.fetchall()

    # todo: return example of each phrase occurrence?
    def representative_phrases(self, doc_ids, limit=10):
        """Return phrases representative of given set of documents.
        
        'Representative' means that the phrases are more common
        within the document set than they are in the corpus as a whole.
        
        Result is list of (phrase ID, phrase text, [document IDs in which phrase occurs]).
        """
        
        if not doc_ids:
            return []
        
        sorted_doc_ids = sorted(doc_ids)
        
        # note: this query is pulling back the text for all phrases,
        # even though only `limit` will be needed. If this is slow
        # may be faster to run a second query to pull back only
        # the text of the final few phrases.
        self.cursor.execute("""
            with candidate_phrases as (
                    select corpus_id, phrase_id
                    from phrase_occurrences
                    where
                        corpus_id = %(corpus_id)s
                        and document_id in %(doc_ids)s
                    group by corpus_id, phrase_id
                )
            select phrase_id, phrase_text, array_agg(document_id order by document_id)
            from candidate_phrases
            inner join phrase_occurrences using (corpus_id, phrase_id)
            inner join phrases using (corpus_id, phrase_id)
            group by phrase_id, phrase_text
        """, dict(corpus_id=self.id, doc_ids=tuple(doc_ids), limit=limit))
        
        weighted_phrase_sets = [(phrase_id, jaccard(docs_with_phrase, sorted_doc_ids), text) for (phrase_id, text, docs_with_phrase) in self.cursor.fetchall()]
        weighted_phrase_sets.sort(key=lambda (id, score, text): score, reverse=True)
        
        return weighted_phrase_sets[:limit]


    def _get_similarities(self):
        # todo: this could be made more efficient with a custom pickling method
        similarities = cache.get('similarities-%s' % self.id)
        if similarities is None:
            self.cursor.execute("""
                    select low_document_id, high_document_id, similarity
                    from similarities
                    where
                        corpus_id = %s
                        and similarity >= 0.5 -- todo: lower bound should be set at ingestion time, not here
                    order by similarity desc
            """, [self.id])
            similarities = self.cursor.fetchall()
            cache.set('similarities-%s' % self.id, similarities)

        return similarities

    def clusters(self, min_similarity):
        """Return clustering of subset of corpus with similarity above given threshold.
        
        Two documents are clustered if they are linked through a sequence of documents
        with similarity above the given threshold. Put another way, the clustering is
        the set of connected components of the similarity graph above the cutoff.
        
        Result is a list of list of document IDs.
        """
        
        all_edges = self._get_similarities()
        # todo: this could be made more efficient by doing a binary search to last
        # edge above cutoff and then taking a slice.
        edges = [(x, y) for (x, y, sim) in all_edges if sim >= min_similarity]
        vertices = set([x for (x, y) in edges] + [y for (x, y) in edges])
        
        partition = Partition(vertices)
        
        for (x, y) in edges:
            partition.merge(x, y)
            
        return partition.sets()

    def clusters_for_doc(self, doc_id):
        """Return the size of the cluster the given doc is in at different cutoffs."""
        
        edges = self._get_similarities()
        vertices = set([x for (x, y, _) in edges] + [y for (x, y, _) in edges])
        partition = Partition(vertices)
        
        result = []
        num_edges = len(edges)
        i = 0
        for c in range(1, 11):
            cutoff = 1.0 - c * 0.05
            while i < num_edges and edges[i][2] >= cutoff:
                partition.merge(edges[i][0], edges[i][1])
                i += 1
            result.append((cutoff, len(partition.group(doc_id))))

        return result

    def similar_docs(self, doc_id, min_similarity=0.5):
        """Return all documents similar to the given document.
        
        Result is list of (document ID, similarity), sorted by similarity.
        """

        self.cursor.execute("""
            with recursive cluster (doc_id) as (
                    select %(target_doc_id)s
                union
                    select case when doc_id = low_document_id then high_document_id else low_document_id end
                    from similarities
                    inner join cluster on doc_id = low_document_id or doc_id = high_document_id
                    where
                        corpus_id = %(corpus_id)s
                        and similarity >= %(min_similarity)s
            )
            select doc_id, similarity
            from cluster
            inner join similarities on
                (low_document_id = %(target_doc_id)s and high_document_id = doc_id)
                or (low_document_id = doc_id and high_document_id = %(target_doc_id)s)
            where
                corpus_id = %(corpus_id)s
            order by similarity desc
        """, dict(corpus_id=self.id, target_doc_id=doc_id, min_similarity=min_similarity))

        return self.cursor.fetchall()
    
    def phrase_overlap(self, target_doc_id, doc_set):
        """Return the number of times each phrase in the given document
        is used in the document set.
        
        Used in conjunction with similar_docs() or clusters().
        By calling repeatedly with different similarity cutoffs, the user can see
        at what point a particular phrase stops being common in the set.
        
        Returns dictionary of phrase ID -> count & character offset.
        """
        
        self.cursor.execute("""
            with phrases_in_target_doc as (
                    select phrase_id, indexes
                    from phrase_occurrences
                    where
                        corpus_id = %(corpus_id)s
                        and document_id = %(target_doc_id)s
                )
            select phrase_id, t.indexes, count(*)
            from phrases_in_target_doc t
            left join phrase_occurrences using (phrase_id)
            where
                corpus_id = %(corpus_id)s
                and document_id in %(doc_set)s
            group by phrase_id, t.indexes
        """, dict(corpus_id=self.id, target_doc_id=target_doc_id, doc_set=tuple(doc_set)))
        
        return dict([(id, dict(indexes=indexes, count=count)) for (id, indexes, count) in self.cursor])

