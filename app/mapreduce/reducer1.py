#!/usr/bin/env python
import sys
from cassandra.cluster import Cluster
from collections import defaultdict

sys.stderr.write("Cassandra")

cluster = Cluster(['cassandra-server'])
session = cluster.connect('search_engine')

insert_term = session.prepare("INSERT INTO terms (term_text, document_frequency) VALUES (?, ?)")
insert_index = session.prepare("INSERT INTO inverted_index (term_text, doc_id, term_frequency) VALUES (?, ?, ?)")

current_term = None
term_data = defaultdict(lambda: defaultdict(int))
sys.stderr.write("previous")
for row in session.execute("SELECT term_text, doc_id, term_frequency FROM inverted_index"):
    term_data[row.term_text][int(row.doc_id)] += int(row.term_frequency)

def process_term(term, doc_id, count):
    term_data[term][doc_id] += int(count)

sys.stderr.write("processing")
for line in sys.stdin:
    parts = line.strip().split('\t')
    term, doc_id, count = parts
    doc_id = int(doc_id)
    count = int(count)
    if term != current_term:
        if current_term is not None:
            process_term(current_term, int(current_doc_id), int(current_count))
        current_term = term
        current_doc_id = int(doc_id)
        current_count = int(count)
    else:
        current_count = str(int(current_count) + int(count))

if current_term is not None:
    process_term(current_term, int(current_doc_id), int(current_count))

sys.stderr.write("Cassandra")
for term, postings in term_data.items():
    df = len(postings)
    session.execute(insert_term, (term, df))

    for doc_id, tf in postings.items():
        session.execute(insert_index, (term, doc_id, tf))

session.shutdown()
