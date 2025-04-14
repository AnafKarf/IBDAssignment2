#!/usr/bin/env python
import sys
from cassandra.cluster import Cluster

# Connect to Cassandra
cluster = Cluster(['cassandra-server'])
session = cluster.connect('search_engine')

# Prepare statements
insert_doc = session.prepare("INSERT INTO documents (doc_id, title, length, content) VALUES (?, ?, ?, ?)")


def process_document(file_path):
        data = file_path.split('\t')
        if len(data) != 3:
            print("term\t0")
            return
        doc_id, title, content = data
        doc_id = int(doc_id)
        # Tokenize content (simple whitespace tokenizer for example)
        words = content.strip().split()
        length = len(words)
        session.execute(insert_doc, (doc_id, title, length, content))

        # Emit words for processing in reducer
        for word in words:
            print(f"{word}\t{doc_id}\t1")



for line in sys.stdin:
    file_path = line.strip()
    process_document(file_path)

session.shutdown()
