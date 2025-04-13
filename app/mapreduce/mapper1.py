#!/usr/bin/env python
import sys
import re
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from uuid import uuid4
import os

# Connect to Cassandra
cluster = Cluster(['cassandra'])
session = cluster.connect('search_engine')

# Prepare statements
insert_doc = session.prepare("INSERT INTO documents (doc_id, title, length, path) VALUES (?, ?, ?, ?)")
insert_stat = session.prepare("INSERT INTO document_stats (doc_id, avg_doc_length, total_documents) VALUES (?, ?, ?)")


def emit_document_metadata(doc_id, title, length, path):
    # Insert document metadata into Cassandra
    session.execute(insert_doc, (doc_id, title, length, path))
    # For stats, we'll update these in the reducer
    print(f"{doc_id}\t{length}\t{title}\t{path}")


def process_document(file_path):
    # Generate a unique doc_id
    doc_id = uuid4()

    try:
        if file_path.startswith('hdfs://'):
            # HDFS file - we'd use Hadoop's file system API in real implementation
            # For this example, we'll assume it's a local file for simplicity
            file_content = open(file_path.replace('hdfs://', ''), 'r').read()
        else:
            # Local file
            file_content = open(file_path, 'r').read()

        # Extract title (first line) and content (rest)
        lines = file_content.split('\n')
        title = lines[0] if lines else "Untitled"
        content = ' '.join(lines[1:])

        # Tokenize content (simple whitespace tokenizer for example)
        words = re.findall(r'\w+', content.lower())
        length = len(words)

        # Emit document metadata
        emit_document_metadata(doc_id, title, length, file_path)

        # Emit words for processing in reducer
        for word in words:
            print(f"{word}\t{doc_id}\t1")

    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}", file=sys.stderr)


for line in sys.stdin:
    file_path = line.strip()
    process_document(file_path)