from collections import defaultdict

try:
    # needed if we're running under PyPy
    import numpypy
except:
    pass
import numpy

import psycopg2.extras
from django.db import connection
from django.core.cache import cache

from partition import Partition
from cluster.ngrams import jaccard
from utils import binary_search

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
        (xs, ys, sims) = self._get_similarities()
        
        sum_accumulator = dict([(id, 0) for id in doc_ids])

        for i in range(len(xs)):
            if xs[i] in sum_accumulator and ys[i] in sum_accumulator:
                sum_accumulator[xs[i]] += sims[i]
                sum_accumulator[ys[i]] += sims[i]
        
        scores = sum_accumulator.items()
        scores.sort(key=lambda (id, score): score, reverse=True)
        
        self.cursor.execute("""
            select document_id, metadata
            from documents
            where
                corpus_id = %(corpus_id)s
                and document_id in %(doc_ids)s
        """, dict(corpus_id=self.id, doc_ids=tuple(doc_ids)))
        
        metadatas = dict(self.cursor.fetchall())
        
        doc_count = float(len(doc_ids))
        
        return [(id, score / doc_count, metadatas[id]) for (id, score) in scores if score > 0]
        
    
    def docs_by_centrality_sql(self, doc_ids):
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

    def representative_phrases_allsql(self, doc_ids, limit=10):
        """Return phrases representative of given set of documents.

        'Representative' means that the phrases are more common
        within the document set than they are in the corpus as a whole.

        Result is list of (phrase ID, phrase text, [document IDs in which phrase occurs]).
        """

        if not doc_ids:
            return []

        self.cursor.execute("""
            select p.phrase_id, score, substring(text for (o.indexes[1].end - o.indexes[1].start) from o.indexes[1].start + 1)
            from (
                select phrase_id, intersection::float / (%(target_size)s + count(distinct document_id) - intersection) as score, example_doc_id
                from (
                    select phrase_id, count(distinct document_id) as intersection, min(document_id) as example_doc_id
                    from phrase_occurrences
                    where
                        corpus_id = %(corpus_id)s
                        and document_id in %(doc_ids)s
                        group by phrase_id
                ) candidate_phrases
                inner join (select * from phrase_occurrences where corpus_id = %(corpus_id)s) all_docs using (phrase_id)
                group by phrase_id, intersection, example_doc_id
                order by score desc
                limit %(limit)s
            ) p
            inner join documents d on d.corpus_id = %(corpus_id)s and d.document_id = p.example_doc_id
            inner join phrase_occurrences o on o.corpus_id = %(corpus_id)s and o.phrase_id = p.phrase_id and o.document_id = p.example_doc_id
        """, dict(corpus_id=self.id, doc_ids=tuple(doc_ids), target_size=len(doc_ids), limit=limit))
        
        return self.cursor.fetchall()


    def _get_phrases(self):
        # retrieve size of each phrase set
        self.cursor.execute("""
            select phrase_id, count(distinct document_id)
            from phrase_occurrences
            where
                corpus_id = %(corpus_id)s
            group by phrase_id
        """, dict(corpus_id=self.id))
        
        phrase_sizes = dict(self.cursor.fetchall())
        
        self.cursor.execute("""
            select document_id, array_agg(phrase_id)
            from phrase_occurrences
            where
                corpus_id = %(corpus_id)s
            group by document_id
            """, dict(corpus_id=self.id))

        doc_phrases = dict(self.cursor.fetchall())
        
        return (doc_phrases, phrase_sizes)

    def representative_phrases_prefetch(self, phrases_data, doc_ids, limit=10):
        """Return phrases representative of given set of documents.
        
        'Representative' means that the phrases are more common
        within the document set than they are in the corpus as a whole.
        
        Result is list of (phrase ID, phrase text, [document IDs in which phrase occurs]).
        """
        
        target_size = len(doc_ids)
        (doc_phrases, phrase_sizes) = phrases_data
        
        phrase_intersections = defaultdict(int)
        for doc_id in doc_ids:
            for phrase_id in doc_phrases[doc_id]:
                phrase_intersections[phrase_id] += 1
        
        # weight each with jaccard measure
        weighted_phrase_sets = [(phrase_id, float(intersection) / (phrase_sizes[phrase_id] + target_size - intersection)) for (phrase_id, intersection) in phrase_intersections.iteritems()]
        weighted_phrase_sets.sort(key=lambda (id, score): score, reverse=True)
        final_phrases = weighted_phrase_sets[:limit]
        
        # pull back example text for the top results
        # self.cursor.execute("""
        #     select target.phrase_id, substring(d.text for (o.indexes[1].end - o.indexes[1].start) from o.indexes[1].start + 1)
        #     from (values %s) target (corpus_id, phrase_id, document_id)
        #     inner join documents d using (corpus_id, document_id)
        #     inner join phrase_occurrences o using (corpus_id, phrase_id, document_id)
        # """ % ",".join(["(%s, %s, %s)" % (self.id, phrase_id, example_doc_id) for (phrase_id, score, example_doc_id) in final_phrases]))
        # 
        # examples = dict(self.cursor.fetchall())
        
        return [(phrase_id, score, "") for (phrase_id, score) in final_phrases]

    def _get_similarities(self, min_sim=None):
        cached = cache.get('analysis.corpus.similarities-%s' % self.id)
        if cached:
            xs = numpy.fromstring(cached[0], numpy.int32)
            ys = numpy.fromstring(cached[1], numpy.int32)
            sims = numpy.fromstring(cached[2], numpy.float32)
        
        else:
            self.cursor.execute("""
                    select low_document_id, high_document_id, similarity
                    from similarities
                    where
                        corpus_id = %s
                        and similarity >= 0.5 -- todo: lower bound should be set at ingestion time, not here
                    order by similarity desc
            """, [self.id])
        
            i = 0
            xs = numpy.empty(self.cursor.rowcount, numpy.uint32)
            ys = numpy.empty(self.cursor.rowcount, numpy.uint32)
            sims = numpy.empty(self.cursor.rowcount, numpy.float32)
            for (x, y, s) in self.cursor.fetchall():
                xs[i], ys[i], sims[i] = x, y, s
                i += 1
        
            cache.set('analysis.corpus.similarities-%s' % self.id, (xs.tostring(), ys.tostring(), sims.tostring()))

        if min_sim:
            sims = sims[:binary_search(sims, min_sim)]
            xs = xs[:len(sims)]
            ys = ys[:len(sims)]
        
        # conversion back to Python ints makes follow up
        # computations much faster in PyPy
        xs = [int(x) for x in xs]
        ys = [int(y) for y in ys]

        return (xs, ys, sims)
        
    def clusters(self, min_similarity):
        """Return clustering of subset of corpus with similarity above given threshold.
        
        Two documents are clustered if they are linked through a sequence of documents
        with similarity above the given threshold. Put another way, the clustering is
        the set of connected components of the similarity graph above the cutoff.
        
        Result is a list of list of document IDs.
        """
        
        xs, ys, _ = self._get_similarities(min_similarity)
        vertices = set(xs + ys)
        
        partition = Partition(vertices)
        
        for i in range(len(xs)):
            partition.merge(xs[i], ys[i])
            
        return partition.sets()

    def hierarchy(self, cutoffs, pruning_size, require_summaries):
        h = cache.get('analysis.corpus.hierarchy-%s-%s-%s' % (self.id, ",".join([str(cutoff) for cutoff in cutoffs]), pruning_size))
        if not h:
            h = self._compute_hierarchy(cutoffs, pruning_size, require_summaries)
            cache.set('analysis.corpus.hierarchy-%s-%s-%s' % (self.id, ",".join([str(cutoff) for cutoff in cutoffs]), pruning_size), h)
            return h
            
        if require_summaries and h[0]['phrases'] == None:
            self._compute_hierarchy_summaries(h)
            cache.set('analysis.corpus.hierarchy-%s-%s-%s' % (self.id, ",".join([str(cutoff) for cutoff in cutoffs]), pruning_size), h)
            
        return h
    
    def _compute_hierarchy_summaries(self, h):
        for cluster in h:
            cluster['phrases'] = [text for (id, score, text) in self.representative_phrases_allsql(cluster['members'], 5)]
            self._compute_hierarchy_summaries(cluster['children'])

    def _compute_hierarchy(self, cutoffs, pruning_size, compute_summaries):
        """Return the hierarchy of clusters, in the format d3 expects.
        
        Cutoffs must be a descending list of floats.
        
        See https://github.com/mbostock/d3/wiki/Partition-Layout for result format.
        
        The documents in a particular cluster can be found by calling
        cluster() using the "name" and "cutoff" values from
        the hierarchy.
        """
        
        xs, ys, sims = self._get_similarities()
        partition = Partition(set(xs + ys))
        
        hierarchy = {}
        num_edges = len(sims)
        i = 0
        for c in cutoffs:
            while i < num_edges and sims[i] >= c:
                partition.merge(xs[i], ys[i])
                i += 1
            
            new_hierarchy = [
                {'name':partition.representative(doc_ids[0]),
                 'size': len(doc_ids),
                 'members': doc_ids,
                 'children': [],
                 'cutoff': c,
                 'phrases': [text for (id, score, text) in self.representative_phrases_allsql(doc_ids, 5)]
                            if compute_summaries else None
                }
                for doc_ids in partition.sets()
                if len(doc_ids) > pruning_size
            ]
            
            for prev_cluster in hierarchy:
                for cluster in new_hierarchy:
                    if partition.representative(prev_cluster['name']) == cluster['name']:
                        cluster['children'].append(prev_cluster)
                        
            hierarchy = new_hierarchy
            
        return hierarchy

    def cluster(self, doc_id, cutoff):
        """Return the set of document IDs in the cluster containing given doc at given cutoff.
        
        hierarchy() method should be used first to get the overview of clusters. This
        method can then be called with a particular doc ID and cutoff from the hierarchy.
        """
        
        xs, ys, sims = self._get_similarities()
        partition = Partition(set(xs + ys))

        num_edges = len(sims)
        i = 0
        while i < num_edges and sims[i] >= cutoff:
            partition.merge(xs[i], ys[i])
            i += 1
            
        return (partition.representative(doc_id), partition.group(doc_id))

    def clusters_for_doc(self, doc_id):
        """Return the size of the cluster the given doc is in at different cutoffs."""
        
        xs, ys, sims = self._get_similarities()
        vertices = set(xs + ys)
        partition = Partition(vertices)
        
        result = []
        num_edges = len(sims)
        i = 0
        for c in range(1, 11):
            cutoff = 1.0 - c * 0.05
            while i < num_edges and sims[i] >= cutoff:
                partition.merge(xs[i], ys[i])
                i += 1
            result.append((cutoff, partition.representative(doc_id), len(partition.group(doc_id))))

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
        
        return dict([(id, dict(indexes=indexes, count=count)) for (id, indexes, count) in self.cursor.fetchall()])

