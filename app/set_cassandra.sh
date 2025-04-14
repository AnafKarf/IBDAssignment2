#!/bin/bash


apt-get update && apt-get install -y python3-pip|
pip3 install cqlsh

CQL_FILE="/tmp/search_engine_schema.cql"

cat > $CQL_FILE << 'EOL'
-- Create keyspace
DROP KEYSPACE IF EXISTS search_engine;
CREATE KEYSPACE IF NOT EXISTS search_engine
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Use the keyspace
USE search_engine;

-- Table for terms vocabulary
CREATE TABLE IF NOT EXISTS terms (
    term_text text PRIMARY KEY,
    document_frequency int
);

-- Table for document metadata
CREATE TABLE IF NOT EXISTS documents (
    doc_id int PRIMARY KEY,
    title text,
    length int,
    content text
);

-- Inverted index table
CREATE TABLE IF NOT EXISTS inverted_index (
    term_text text,
    doc_id int,
    term_frequency int,
    PRIMARY KEY (term_text, doc_id)
) WITH CLUSTERING ORDER BY (doc_id ASC);
EOL

echo "Setting up Cassandra database schema..."
cqlsh "cassandra-server" -f $CQL_FILE

rm $CQL_FILE

echo "Cassandra database setup complete."
