#!/usr/bin/env python
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
import sys
import math

# Constants for BM25
K1 = 1.2
B = 0.75


def calculate_bm25(query_terms, doc_id, doc_length, avg_doc_length, total_documents):
    cluster = Cluster(['cassandra'])
    session = cluster.connect('search_engine')

    score = 0.0

    for term in query_terms:
        # Get document frequency for term
        term_df = session.execute(
            "SELECT document_frequency FROM terms WHERE term_text = %s", (term,)
        ).one()

        if not term_df:
            continue

        df = term_df.document_frequency

        # Get term frequency in this document
        tf_row = session.execute(
            "SELECT term_frequency FROM inverted_index WHERE term_text = %s AND doc_id = %s",
            (term, doc_id)
        ).one()

        tf = tf_row.term_frequency if tf_row else 0

        # Calculate IDF
        idf = math.log((total_documents - df + 0.5) / (df + 0.5) + 1)

        # Calculate TF component
        tf_component = (tf * (K1 + 1)) / (tf + K1 * (1 - B + B * (doc_length / avg_doc_length)))

        score += idf * tf_component

    session.shutdown()
    return score


def main():
    if len(sys.argv) < 2:
        print("Please provide a query as an argument.")
        sys.exit(1)

    query = sys.argv[1].lower().split()

    # Initialize Spark
    conf = SparkConf().setAppName("SearchEngineQuery")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)

    # Connect to Cassandra to get document stats
    cluster = Cluster(['cassandra'])
    session = cluster.connect('search_engine')

    # Get average document length and total documents
    stats_row = session.execute(
        "SELECT avg_doc_length, total_documents FROM document_stats LIMIT 1"
    ).one()

    if not stats_row:
        print("No documents in index.")
        sys.exit(1)

    avg_doc_length = stats_row.avg_doc_length
    total_documents = stats_row.total_documents

    # Get all documents with their lengths
    documents = session.execute("SELECT doc_id, length, title FROM documents")

    # Convert to RDD
    docs_rdd = sc.parallelize([(str(row.doc_id), row.length, row.title) for row in documents])

    # Calculate BM25 scores for each document
    scores_rdd = docs_rdd.map(lambda x: (
        x[0],  # doc_id
        x[2],  # title
        calculate_bm25(query, x[0], x[1], avg_doc_length, total_documents)
    ))

    # Get top 10 documents
    top_docs = scores_rdd.takeOrdered(10, key=lambda x: -x[2])

    # Print results
    print("\nTop 10 relevant documents:")
    print("--------------------------")
    for i, (doc_id, title, score) in enumerate(top_docs, 1):
        print(f"{i}. {title} (ID: {doc_id}, Score: {score:.4f})")

    sc.stop()


if __name__ == "__main__":
    main()