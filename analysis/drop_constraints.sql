-- drop the "non functional" constraints--those which only serve an error checking purpose.
-- primary key constraints are left, since they add indexes necessary for performance.

alter table similarities drop constraint "similarities_check";
alter table similarities drop constraint "similarities_corpus_id_fkey";
alter table similarities drop constraint "similarities_corpus_id_fkey1";
alter table similarities drop constraint "similarities_corpus_id_fkey2";

alter table phrase_occurrences drop constraint "phrase_occurrences_corpus_id_fkey";
alter table phrase_occurrences drop constraint "phrase_occurrences_corpus_id_fkey1";
alter table phrase_occurrences drop constraint "phrase_occurrences_corpus_id_fkey2";

alter table phrases drop constraint "phrases_corpus_id_phrase_text_key";
alter table phrases drop constraint "phrases_corpus_id_fkey";

alter table documents drop constraint "documents_corpus_id_fkey";
