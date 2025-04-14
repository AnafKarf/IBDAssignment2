#!/usr/bin/env python
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
import sys
import math

K1 = 1
B = 0.75
cluster = Cluster(['cassandra-server'])
session = cluster.connect('search_engine')


def calculate_bm25(query_terms, doc_id, doc_length, avg_doc_length, total_documents):

    score = 0.0

    for term in query_terms:
        term_df = session.execute(
            "SELECT document_frequency FROM terms WHERE term_text = %s", (term,)
        ).one()

        if not term_df:
            continue

        df = term_df.document_frequency

        tf_row = session.execute(
            "SELECT term_frequency FROM inverted_index WHERE term_text = %s AND doc_id = %s",
            (term, doc_id)
        ).one()

        tf = tf_row.term_frequency if tf_row else 0

        score += math.log(total_documents / df) * ((K1 + 1) * tf) / (K1 * ((1 - B) + B * doc_length / avg_doc_length) + tf)

    return score


if len(sys.argv) < 2:
    print("Please provide a query as an argument.")
    sys.exit(1)

query = sys.argv[1].lower().split()

conf = SparkConf().setAppName("SearchEngineQuery")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

avg_doc_length = session.execute("SELECT AVG(length) FROM documents").one().system_avg_length
total_documents = session.execute("SELECT COUNT(*) FROM documents").one().count

documents = session.execute("SELECT doc_id, length, title FROM documents")

docs_rdd = [(int(row.doc_id), int(row.length), row.title) for row in documents]

scores_rdd = [(x[0], x[2], calculate_bm25(query, x[0], x[1], avg_doc_length, total_documents)) for x in docs_rdd]

top_docs = sorted(scores_rdd, key=lambda x: -x[2])[:10]

print("query:", sys.argv[1])
print("\nTop 10 relevant documents:")
print("--------------------------")
for i, (doc_id, title, score) in enumerate(top_docs, 1):
    print(f"{i}. {title} (ID: {doc_id}, Score: {score:.4f})")

sc.stop()

session.shutdown()
