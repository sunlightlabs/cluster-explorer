import itertools
import random

import psycopg2.extras
from django.db import connection
from django.core.cache import cache

from django.conf import settings
if getattr(settings, "USE_C_PARTITION", False):
    print "Using C partition"
    from cpartition import cPartition as Partition
else:
    from partition import Partition

from utils import profile
import bsims

# Django connection is a wrappers around psycopg2 connection,
# but that wrapped object isn't initialized till a call is made.
connection.cursor()
psycopg2.extras.register_composite('int_bounds', connection.connection)
psycopg2.extras.register_hstore(connection.connection)


def get_dual_corpora_by_metadata(key, value):
    c = connection.cursor()
    c.execute("select corpus_id, metadata->'parser' from corpora where metadata -> %s = %s", [key, value])
    corpora = c.fetchall()
    if not corpora:
        return None
        
    ngram_corpora = [id for (id, parser) in corpora if parser=='4-gram']
    sentence_corpora = [id for (id, parser) in corpora if parser=='sentence']
    
    # preferrence 4-grams for similarity, sentence for phrases, otherwise use arbitrary result
    return Corpus(
        corpus_id = ngram_corpora[0] if ngram_corpora else corpora[0][0],
        sentence_corpus_id = sentence_corpora[0] if sentence_corpora else None
    )
    

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
    
    def __init__(self, corpus_id=None, metadata={}, sentence_corpus_id=None):
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

        self.sentence_corpus_id = sentence_corpus_id if sentence_corpus_id else corpus_id

        # hierarchy settings are fixed here, but client could in theory change them if desred
        self.hierarchy_cutoffs = bsims.STORED_SIMILARITY_CUTOFFS


    def num_docs(self):
        self.cursor.execute("select count(*) from documents where corpus_id=%s", [self.id])
        return self.cursor.fetchone()[0]

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
        return dict(self.cursor.fetchall())
    
    def delete(self, doc_ids):
        """Remove all data associated with given doc IDs."""

        # psycopg2 requires a tuple
        if not isinstance(doc_ids, tuple):
            doc_ids = tuple(doc_ids)

        self.cursor.execute("""
            delete from phrase_occurrences
            where
                corpus_id = %(corpus_id)s
                and document_id in %(doc_ids)s
        """, dict(corpus_id=self.id, doc_ids=doc_ids))

        self.cursor.execute("""
            delete from phrases
            where
                corpus_id = %(corpus_id)s
                and phrase_id not in (
                    select distinct phrase_id
                    from phrase_occurrences
                    where
                        corpus_id = %(corpus_id)s
                )
        """, dict(corpus_id=self.id, doc_ids=doc_ids))

        self.cursor.execute("""
            delete from documents
            where
                corpus_id = %(corpus_id)s
                and document_id in %(doc_ids)s
        """, dict(corpus_id=self.id, doc_ids=doc_ids))

        # remove from the similarities file store
        bsims.remove_documents(self.id, doc_ids)

    def delete_corpus(self):
        """Remove all data associated with given doc IDs."""

        # psycopg2 requires a tuple
        ids = tuple(set([self.id, self.sentence_corpus_id]))

        self.cursor.execute("""
            delete from phrase_occurrences
            where
                corpus_id in %(corpus_id)s
        """, dict(corpus_id=ids))

        self.cursor.execute("""
            delete from phrases
            where
                corpus_id in %(corpus_id)s
        """, dict(corpus_id=ids))

        self.cursor.execute("""
            delete from documents
            where
                corpus_id in %(corpus_id)s
        """, dict(corpus_id=ids))

        self.cursor.execute("""
            delete from corpora
            where
                corpus_id in %(corpus_id)s
        """, dict(corpus_id=ids))

        # remove from the similarities file store
        bsims.remove_all(self.id)

    def delete_by_metadata(self, key, values):
        """Remove all documents where a given key is in the given values."""

        self.cursor.execute("""
            select document_id
            from documents
            where
                corpus_id = %(corpus_id)s
                and metadata -> %(key)s in %(values)s
        """, dict(corpus_id=self.id, key=key, values=tuple(values)))

        doc_ids = [id for (id,) in self.cursor.fetchall()]

        if len(doc_ids) > 0:
            self.delete(doc_ids)

    
    ### document retrieval methods for clients ###
    
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

    def doc_metadatas(self, doc_ids):
        """Return a list of doc_ids and metadata values for each doc ID."""

        self.cursor.execute("""
            select document_id, metadata
            from documents
            where
                corpus_id = %s
                and document_id in %s
        """, [self.id, tuple(doc_ids)])

        return self.cursor.fetchall()
        
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


    ### methods returning information about clustering ###

    @profile
    def _representative_phrases(self, doc_ids, limit=10):
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
        """, dict(corpus_id=self.sentence_corpus_id, doc_ids=tuple(doc_ids), target_size=len(doc_ids), limit=limit))
        
        return self.cursor.fetchall()


    @profile
    def get_similarities(self):
        return bsims.numpy_deserialize(bsims.file_get(self.id))


    @profile
    def add_similarities(self, new_sims):
        existing_sims = self.get_similarities()
        sims = existing_sims + new_sims
        sims.sort(key=lambda (x, y, s): s, reverse=True)
        bsims.file_set(self.id, bsims.numpy_serialize(sims))


    @profile
    def hierarchy(self, require_summaries=False):
        h = cache.get(self._hierarchy_cache_key())
        if not h:
            h = self._compute_hierarchy(require_summaries)
            cache.set(self._hierarchy_cache_key(), h)
            return h
            
        if require_summaries and h[0]['phrases'] == None:
            self._compute_hierarchy_summaries(h)
            cache.set(self._hierarchy_cache_key(), h)
            
        return h

    def update_hierarchy_cache(self):
        """If corpus' hierarchy is already cached, then recompute."""
        h = cache.get(self._hierarchy_cache_key())
        if h:
            new_h = self._compute_hierarchy(True)
            cache.set(self._hierarchy_cache_key(), new_h)
    
    def delete_hierarchy_cache(self):
        """Delete hierarchy cache if it exists."""
        cache.delete(self._hierarchy_cache_key())

    def _hierarchy_cache_key(self):
        return 'analysis.corpus.hierarchy-%s-%s' % (self.id, ",".join([str(cutoff) for cutoff in self.hierarchy_cutoffs]))

    
    @profile
    def _compute_hierarchy_summaries(self, h):
        for cluster in h:
            cluster['phrases'] = [text for (id, score, text) in self._representative_phrases(cluster['members'], 5)]
            self._compute_hierarchy_summaries(cluster['children'])

    @profile
    def _compute_hierarchy(self, compute_summaries):
        """Return the hierarchy of clusters, in the format d3 expects.
                
        See https://github.com/mbostock/d3/wiki/Partition-Layout for result format.
    
        The functions find_doc_in_hierarchy() and trace_doc_in_hierarchy() can return
        more information about the output of _compute_hierarchy().
        """
        
        self.cursor.execute("select document_id from documents where corpus_id = %s", [self.id])
        all_docs = set([d for (d,) in self.cursor.fetchall()])
        partition = Partition(all_docs)

        pruning_size = max(2, len(all_docs) / 100);
        hierarchy = {}

        similarity_reader = bsims.get_similarity_reader(self.id)
        # short-circuit the python-side computation if everything lines up right
        if similarity_reader isinstance(similarity_reader, LZ4SimilarityReader) and hasattr(partition, "merge_lz4"):
            # we can use the C stuff
            out = _compute_hierarchy_fast(compute_summaries, similarity_reader, partition, pruning_size)
            partition.free()
            return out

        cutoffs_remaining = list(self.hierarchy_cutoffs)
        # note: (-1,-1,0) terminator is necessary so that hierarchy building
        # code has a chance to run one more time after last sim is read.
        for (x, y, sim) in itertools.chain(similarity_reader, [(-1,-1,0)]):
            while cutoffs_remaining and sim < cutoffs_remaining[0]:
                new_hierarchy = [
                    {'name':partition.representative(doc_ids[0]),
                     'size': len(doc_ids),
                     'members': doc_ids,
                     'children': [],
                     'cutoff': cutoffs_remaining[0],
                     'phrases': [text for (id, score, text) in self._representative_phrases(doc_ids, 5)]
                                if compute_summaries else None
                    }
                    for doc_ids in partition.sets()
                    if len(doc_ids) > pruning_size
                ]
                
                for prev_cluster in hierarchy:
                    for cluster in new_hierarchy:
                        if partition.representative(prev_cluster['name']) == cluster['name']:
                            cluster['children'].append(prev_cluster)
                
                for cluster in new_hierarchy:
                    _order_members(cluster)

                hierarchy = new_hierarchy
                cutoffs_remaining.pop(0)
            
            if not cutoffs_remaining:
                break

            partition.merge(x, y)

        partition.free()
        return hierarchy

    def _compute_hierarchy_fast(compute_summaries, similarity_reader, partition, pruning_size):
        print "Using fast hierarchy"
        hierarchy = {}
        for cutoff, files in similarity_reader.files_by_cutoff():
            for cfile in files:
                partition.merge_lz4(cfile)

            new_hierarchy = [
                {'name':partition.representative(doc_ids[0]),
                 'size': len(doc_ids),
                 'members': doc_ids,
                 'children': [],
                 'cutoff': cutoff,
                 'phrases': [text for (id, score, text) in self._representative_phrases(doc_ids, 5)]
                            if compute_summaries else None
                }
                for doc_ids in partition.sets()
                if len(doc_ids) > pruning_size
            ]
            
            for prev_cluster in hierarchy:
                for cluster in new_hierarchy:
                    if partition.representative(prev_cluster['name']) == cluster['name']:
                        cluster['children'].append(prev_cluster)
            
            for cluster in new_hierarchy:
                _order_members(cluster)

            hierarchy = new_hierarchy

        return hierarchy



    def phrase_overlap(self, target_doc_id, doc_set):
        """Return the number of times each phrase in the given document
        is used in a sample of the document set.
        
        Returns dictionary of phrase ID -> count & character offset.
        """

        # since we're looking for phrases that are common in the
        # document set, it's fine to sample. This makes problem
        # tractable for large clusters.
        if len(doc_set) > 100:
            sampling_multiplier = len(doc_set) / 100.0
            doc_set = random.sample(doc_set, 100)
        else:
            sampling_multiplier = 1

        
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
        
        return dict([(id, dict(indexes=indexes, count=int(sampling_multiplier *count)))
                    for (id, indexes, count) in self.cursor.fetchall()])


def _order_members(cluster):

    child_lists = [child['members'] for child in cluster['children']]
    child_lists.sort(key=lambda l: len(l), reverse=True)
    ordered_children = reduce(lambda x, y: x + y, child_lists, [])

    all_members = set(cluster['members'])
    child_set = set(ordered_children)
    new_members = [m for m in all_members if m not in child_set]

    cluster['members'] = ordered_children + new_members


def find_doc_in_hierarchy(hierarchy, doc_id, cutoff):
    """ Return the members of the cluster where doc_id is found at given cutoff.

    Returns empty list if doc_id not clustered at given cutoff.
    """

    for cluster in hierarchy:
        if doc_id in cluster['members']:
            if cluster['cutoff'] == cutoff:
                return cluster

            if cluster['cutoff'] < cutoff:
                return find_doc_in_hierarchy(cluster['children'], doc_id, cutoff)

    return None

def trace_doc_in_hierarchy(hierarchy, doc_id):

    for cluster in hierarchy:
        if doc_id in cluster['members']:
            stats = (cluster['cutoff'], cluster['name'], cluster['size'])
            return [stats] + trace_doc_in_hierarchy(cluster['children'], doc_id)

    return []


