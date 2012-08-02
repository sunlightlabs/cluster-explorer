

create table corpora (
    corpus_id serial PRIMARY KEY,
    metadata hstore
);

create table documents (
    corpus_id integer REFERENCES corpora(corpus_id) ON DELETE CASCADE,
    document_id integer,
    text text,
    -- to add: ingestion date
    -- to add: index on document ID metadata?
    metadata hstore,
    PRIMARY KEY (corpus_id, document_id)
);

create table phrases (
    corpus_id integer REFERENCES corpora(corpus_id) ON DELETE CASCADE,
    phrase_id integer,
    phrase_text varchar,
    PRIMARY KEY (corpus_id, phrase_id),
    UNIQUE (corpus_id, phrase_text)
);


create type int_bounds as (start integer, "end" integer);

create table phrase_occurrences (
    corpus_id integer REFERENCES corpora(corpus_id) ON DELETE CASCADE,
    document_id integer,
    phrase_id integer,
    indexes int_bounds[],
    PRIMARY KEY (corpus_id, document_id, phrase_id),
    FOREIGN KEY (corpus_id, document_id) REFERENCES documents(corpus_id, document_id) ON DELETE CASCADE DEFERRABLE,
    FOREIGN KEY (corpus_id, phrase_id) REFERENCES phrases (corpus_id, phrase_id) DEFERRABLE
);

create table similarities (
    corpus_id integer REFERENCES corpora(corpus_id) ON DELETE CASCADE,
    low_document_id integer,
    high_document_id integer,
    similarity real,
    PRIMARY KEY (corpus_id, low_document_id, high_document_id),
    CHECK (low_document_id < high_document_id),
    FOREIGN KEY (corpus_id, low_document_id) REFERENCES documents(corpus_id, document_id) ON DELETE CASCADE DEFERRABLE,
    FOREIGN KEY (corpus_id, high_document_id) REFERENCES documents(corpus_id, document_id) ON DELETE CASCADE DEFERRABLE
);

create index similarities_corpus_low_id on similarities (corpus_id, low_document_id);
create index similarities_corpus_high_id on similarities (corpus_id, high_document_id);
create index similarities_similarity on similarities (corpus_id, similarity);


