#!/usr/bin/env python
from pyspark import SparkContext, SparkConf
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
import socket
import sys
import math

K1 = 1
B = 0.75


class CassandraHelper:
    @staticmethod
    def get_contact_points():
        hosts = ['cassandra-server', 'localhost', '127.0.0.1']
        for host in hosts:
            try:
                socket.gethostbyname(host)
                return [host]
            except socket.gaierror:
                continue
        raise Exception("Could not resolve any Cassandra contact points")

    @staticmethod
    def get_session():
        contact_points = CassandraHelper.get_contact_points()
        cluster = Cluster(
            contact_points,
            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1'),
            protocol_version=4,
            connect_timeout=30,
            idle_heartbeat_interval=30
        )
        return cluster.connect('search_engine')


def load_documents_rdd(sc):
    session = CassandraHelper.get_session()
    try:
        rows = session.execute("SELECT doc_id, length, title FROM documents")
        return sc.parallelize([(row.doc_id, row.length, row.title) for row in rows])
    finally:
        session.cluster.shutdown()


def get_term_frequencies(query_terms, sc):
    session = CassandraHelper.get_session()
    try:
        term_freq = {}
        for term in query_terms:
            row = session.execute(
                "SELECT document_frequency FROM terms WHERE term_text = %s",
                (term,)
            ).one()
            if row:
                term_freq[term] = row.document_frequency
        return sc.broadcast(term_freq)
    finally:
        session.cluster.shutdown()


def calculate_bm25_partition(partition, query_terms, avg_doc_length, total_docs, term_freq_bcast):
    session = CassandraHelper.get_session()
    try:
        term_freq_map = term_freq_bcast.value
        results = []

        for doc_id, length, title in partition:
            score = 0.0
            for term in query_terms:
                df = term_freq_map.get(term, 0)
                if df == 0:
                    continue

                tf_row = session.execute(
                    "SELECT term_frequency FROM inverted_index WHERE term_text = %s AND doc_id = %s",
                    (term, doc_id)
                ).one()

                tf = tf_row.term_frequency if tf_row else 0
                if tf == 0:
                    continue

                score += math.log(total_docs / df) * ((K1 + 1) * tf) / (K1 * ((1 - B) + B * length / avg_doc_length) + tf)

            results.append((doc_id, title, score))
        return results
    finally:
        session.cluster.shutdown()


def main():
    if len(sys.argv) < 2:
        print("Please provide a query as an argument.")
        sys.exit(1)

    query = sys.argv[1].lower().split()

    conf = SparkConf().setAppName("SearchEngineQuery")
    sc = SparkContext(conf=conf)

    try:
        documents_rdd = load_documents_rdd(sc)
        term_freq_bcast = get_term_frequencies(query, sc)

        session = CassandraHelper.get_session()
        try:
            avg_doc_length = session.execute(
                "SELECT AVG(length) FROM documents"
            ).one().system_avg_length
            total_docs = session.execute(
                "SELECT COUNT(*) FROM documents"
            ).one().count
        finally:
            session.cluster.shutdown()

        scores_rdd = documents_rdd.mapPartitions(
            lambda partition: calculate_bm25_partition(
                partition, query, avg_doc_length, total_docs, term_freq_bcast
            )
        )

        top_docs = scores_rdd.takeOrdered(10, key=lambda x: -x[2])

        print(f"\nQuery: {sys.argv[1]}")
        print("\nTop 10 relevant documents:")
        print("--------------------------")
        for i, (doc_id, title, score) in enumerate(top_docs, 1):
            print(f"{i}. {title} (ID: {doc_id}, Score: {score:.4f})")

    finally:
        sc.stop()


if __name__ == "__main__":
    main()
