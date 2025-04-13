#!/bin/bash

# This script sets up the Cassandra database schema for the search engine

CQL_FILE="/tmp/search_engine_schema.cql"

# Create CQL file with schema definition
cat > $CQL_FILE << 'EOL'
-- Create keyspace
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
    doc_id uuid PRIMARY KEY,
    title text,
    length int,
    path text
);

-- Inverted index table
CREATE TABLE IF NOT EXISTS inverted_index (
    term_text text,
    doc_id uuid,
    term_frequency int,
    PRIMARY KEY (term_text, doc_id)
) WITH CLUSTERING ORDER BY (doc_id ASC);

-- Document statistics for BM25 calculation
CREATE TABLE IF NOT EXISTS document_stats (
    doc_id uuid PRIMARY KEY,
    avg_doc_length double,
    total_documents int
);
EOL

# Execute CQL file
echo "Setting up Cassandra database schema..."
cqlsh -f $CQL_FILE

# Clean up
rm $CQL_FILE

echo "Cassandra database setup complete."