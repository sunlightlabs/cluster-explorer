-- just a note taking area for queries that will eventually form the core algorithms


-- get the phrases for a document
select array_agg(phrase_id)
from (
    select phrase_id
    phrase_occurrences
    where
        corpus_id = ?
        and document_id = ?
    order by phrase_id) x
    
    
-- get all docs above a certain similarity with given doc
select high_document_id as doc_id, similarity
from similarities
where
    low_document_id = ?
    and similarity > ?
union all
select low_document_id as doc_id, similarity
from similarities
where
    high_document_id = ?
    and similarity > ?

-- combine the above two to get the phrases in all similar documents
with similar_documents as <query above>
select doc_id, array_agg(phrase_id), -- could also easily add in the similarity here for client-side filtering
from (
    select document_id, phrase_id
    phrase_occurrences
    where
        corpus_id = ?
        and document_id in (select doc_id from similar_documents)
    order by document_id, phrase_id) x
group by doc_id



-- the ultimate query: portion of documents above given similarity that contain each phrase
with 
    similar_documents as...,
    phrases_in_target_doc as...
select phrase_id, count(*)
from phrases_in_target_doc
left join (
    select document_id, phrase_id
     phrase_occurrences
     where
         corpus_id = ?
         and document_id in (select doc_id from similar_documents)
     order by document_id, phrase_id) x using (phrase_id)
group by phrase_id



