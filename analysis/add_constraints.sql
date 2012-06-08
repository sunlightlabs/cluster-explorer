
-- add in constraints for error checking.
-- should not be present in production, as they significantly slow ingestion
-- may want to add them in testing to ensure correctness

alter table documents add constraint "documents_corpus_id_fkey" FOREIGN KEY (corpus_id) REFERENCES corpora(corpus_id) ON DELETE CASCADE;

alter table phrases add constraint "phrases_corpus_id_phrase_text_key" UNIQUE (corpus_id, phrase_text);
alter table phrases add constraint "phrases_corpus_id_fkey" FOREIGN KEY (corpus_id) REFERENCES corpora(corpus_id) ON DELETE CASCADE;

alter table phrase_occurrences add constraint "phrase_occurrences_corpus_id_fkey" FOREIGN KEY (corpus_id) REFERENCES corpora(corpus_id) ON DELETE CASCADE;
alter table phrase_occurrences add constraint "phrase_occurrences_corpus_id_fkey1" FOREIGN KEY (corpus_id, document_id) REFERENCES documents(corpus_id, document_id) ON DELETE CASCADE DEFERRABLE;
alter table phrase_occurrences add constraint "phrase_occurrences_corpus_id_fkey2" FOREIGN KEY (corpus_id, phrase_id) REFERENCES phrases(corpus_id, phrase_id) DEFERRABLE;

alter table similarities add constraint "similarities_check" CHECK (low_document_id < high_document_id);
alter table similarities add constraint "similarities_corpus_id_fkey" FOREIGN KEY (corpus_id) REFERENCES corpora(corpus_id) ON DELETE CASCADE;
alter table similarities add constraint "similarities_corpus_id_fkey1" FOREIGN KEY (corpus_id, low_document_id) REFERENCES documents(corpus_id, document_id) ON DELETE CASCADE DEFERRABLE;
alter table similarities add constraint "similarities_corpus_id_fkey2" FOREIGN KEY (corpus_id, high_document_id) REFERENCES documents(corpus_id, document_id) ON DELETE CASCADE DEFERRABLE;
