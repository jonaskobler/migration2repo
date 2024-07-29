CREATE TABLE document (
    id uuid primary key default gen_random_uuid(),
    name text not null,
    url text not null,
    status text not null
);

CREATE TABLE clause (
    id uuid primary key default gen_random_uuid(),
    documentId uuid not null,
    section text not null,
    subsection text not null,
    content text not null,

    constraint fk_documentId
        foreign key (documentId)
            references document(id)
            on delete cascade
);