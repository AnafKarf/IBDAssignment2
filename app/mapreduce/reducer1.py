#!/usr/bin/env python
import sys
from cassandra.cluster import Cluster
from uuid import UUID
from collections import defaultdict

# Connect to Cassandra
cluster = Cluster(['cassandra'])
session = cluster.connect('search_engine')

# Prepare statements
insert_term = session.prepare("INSERT INTO terms (term_text, document_frequency) VALUES (?, ?)")
insert_index = session.prepare("INSERT INTO inverted_index (term_text, doc_id, term_frequency) VALUES (?, ?, ?)")

current_term = None
term_data = defaultdict(list)
doc_lengths = []
total_documents = 0
sum_doc_lengths = 0


def process_term(term, doc_id, count):
    term_data[term].append((doc_id, int(count)))


def update_document_stats():
    if doc_lengths:
        avg_length = sum(doc_lengths) / len(doc_lengths)
        # Update stats for all documents (simplification)
        for doc_id, length, title, path in doc_lengths:
            session.execute("UPDATE document_stats SET avg_doc_length = ?, total_documents = ? WHERE doc_id = ?",
                            (avg_length, len(doc_lengths), UUID(doc_id)))



for line in sys.stdin:
    parts = line.strip().split('\t')

    if len(parts) == 4:
            # Document metadata line: doc_id length title path
        doc_id, length, title, path = parts
        doc_lengths.append((doc_id, int(length), title, path))
    elif len(parts) == 3:
            # Term data: term doc_id count
        term, doc_id, count = parts
        if term != current_term:
            if current_term is not None:
                process_term(current_term, current_doc_id, current_count)
            current_term = term
            current_doc_id = doc_id
            current_count = count
        else:
            current_count = str(int(current_count) + int(count))

    # Process the last term
if current_term is not None:
    process_term(current_term, current_doc_id, current_count)

    # Update terms and inverted index in Cassandra
for term, postings in term_data.items():
    df = len(set(doc_id for doc_id, _ in postings))
    session.execute(insert_term, (term, df))

    for doc_id, tf in postings:
        session.execute(insert_index, (term, UUID(doc_id), tf))

    # Update document statistics
update_document_stats()