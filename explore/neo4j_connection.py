from neo4j import GraphDatabase


class Neo4jConnection:
    """
    Class for initiating Neo4j session.
    """

    def __init__(self, uri, user, password, db):
        """
        Init method
        Arguments
        ----------
        uri : str
        user : str
        password: str
        """
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.db = db

    def close(self):
        """
        Closes Driver
        """
        self.driver.close()

    def query(self, query, **kwargs):
        """
        Makes query for initating session
        Arguments
        ----------
        query : query

        Returns
        ----------
        response : list
        """
        with self.driver.session() as session:
            kwargs['database_'] = self.db
            response = list(session.run(query, **kwargs))
            return response
