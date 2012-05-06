

create table corpora (
    corpus_id serial PRIMARY KEY
    -- plus metadata
);

create table documents (
    corpus_id integer REFERENCES corpora(corpus_id),
    document_id integer,
    text text,
    -- plus metadata
    PRIMARY KEY (corpus_id, document_id),
    FOREIGN KEY (corpus_id) REFERENCES corpora (corpus_ID) ON DELETE CASCADE
);

create table phrases (
    corpus_id integer REFERENCES corpora(corpus_id),
    phrase_id integer,
    phrase_text varchar,
    PRIMARY KEY (corpus_id, phrase_id),
    UNIQUE (corpus_id, phrase_text),
    FOREIGN KEY (corpus_id) REFERENCES corpora (corpus_ID) ON DELETE CASCADE
);


create type int_bounds as (start integer, "end" integer);

create table phrase_occurrences (
    corpus_id integer REFERENCES corpora(corpus_id),
    document_id integer,
    phrase_id integer,
    indexes int_bounds[],
    PRIMARY KEY (corpus_id, document_id, phrase_id),
    FOREIGN KEY (corpus_id, document_id) REFERENCES documents(corpus_id, document_id) ON DELETE CASCADE DEFERRABLE,
    FOREIGN KEY (corpus_id, phrase_id) REFERENCES phrases (corpus_id, phrase_id) DEFERRABLE
);

create table similarities (
    corpus_id integer REFERENCES corpora(corpus_id),
    low_document_id integer,
    high_document_id integer,
    similarity real,
    PRIMARY KEY (corpus_id, low_document_id, high_document_id),
    CHECK (low_document_id < high_document_id),
    FOREIGN KEY (corpus_id, low_document_id) REFERENCES documents(corpus_id, document_id) ON DELETE CASCADE DEFERRABLE,
    FOREIGN KEY (corpus_id, high_document_id) REFERENCES documents(corpus_id, document_id) ON DELETE CASCADE DEFERRABLE
);


