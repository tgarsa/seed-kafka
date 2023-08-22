
-- CREATE TABLE Characters
DROP TABLE IF EXISTS characters;
CREATE TABLE characters (
    id SERIAL PRIMARY KEY,
    message_id VARCHAR NOT NULL,
    document_name VARCHAR NOT NULL,
    charac VARCHAR NOT NULL
);

-- CREATE TABLE documents
DROP TABLE IF EXISTS documents;
CREATE TABLE documents (
    id SERIAL PRIMARY KEY,
    message_id VARCHAR NOT NULL,
    document_name VARCHAR NOT NULL,
    time INTEGER NOT NULL,
    readers INTEGER NOT NULL,
    saga VARCHAR NOT NULL
);