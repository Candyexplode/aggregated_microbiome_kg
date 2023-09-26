import logging, sys, os

from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

USER = 'neo4j'
PASSWORD = '1qazXSW@'


class MovieDb:
    """
    Biolerplate code copied from Neo4J Python client documentation
    """
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    @staticmethod
    def enable_log(level, output_stream):
        handler = logging.StreamHandler(output_stream)
        handler.setLevel(level)
        logging.getLogger('neo4j').addHandler(handler)
        logging.getLogger('neo4j').setLevel(level)

    def find_movies(self, actor_name):
        with self.driver.session() as session:
            result = session.execute_read(self._movies_actor_is_in, actor_name)
            for row in result:
                print("Movie: {row}".format(row=row))

    @staticmethod
    def _movies_actor_is_in(tx, actor_name):
        query = (
            "match (actor:Person {name: $actor_name})-[:ACTED_IN]->(movies) "
            "return movies.title as title"
        )
        result = tx.run(query, actor_name=actor_name)
        return [row["title"] for row in result]


if __name__ == "__main__":
    bolt_url = "neo4j://localhost:7687"
    MovieDb.enable_log(logging.INFO, sys.stdout)
    MovieDb = MovieDb(bolt_url, USER, PASSWORD)
    MovieDb.find_movies('Tom Hanks')
    MovieDb.close()