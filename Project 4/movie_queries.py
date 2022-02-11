from neo4j import GraphDatabase, basic_auth
import socket


class Movie_queries(object):
    def __init__(self, password):
        self.driver = GraphDatabase.driver("bolt://localhost", auth=("neo4j", password), encrypted=False)
        self.session = self.driver.session()
        self.transaction = self.session.begin_transaction()

    def q0(self):
        result = self.transaction.run("""
            MATCH (n:Actor) RETURN n.name, n.id ORDER BY n.birthday ASC LIMIT 3
        """)
        return [(r[0], r[1]) for r in result]

    def q1(self):
        result = self.transaction.run("""
            MATCH (n:Actor)-[r:ACTS_IN]->()
            RETURN n.name, count(r) as count ORDER BY count DESC, n.name ASC LIMIT 20
        """)
        return [(r[0], r[1]) for r in result]

    def q2(self):
        result = self.transaction.run("""
            MATCH ()-[r1:RATED]->(m:Movie), ()-[r2:ACTS_IN]->(m:Movie)
            WITH m.title AS title, COUNT(DISTINCT r1) AS ratings, COUNT(DISTINCT r2) AS cast
            WHERE ratings > 0
            RETURN title, cast ORDER BY cast DESC LIMIT 1
        """)

        
        return [(r[0], r[1]) for r in result]

    def q3(self):
        result = self.transaction.run("""
            MATCH (p:Person)-[r:DIRECTED]->(m:Movie)
            WITH p AS p, COUNT(DISTINCT m.genre) AS genres
            WHERE genres >= 2
            RETURN p.name, genres ORDER BY genres DESC, p.name ASC
        """)
        return [(r[0], r[1]) for r in result]

    
    def q4(self):
        result = self.transaction.run("""
            MATCH (kb:Actor{name: "Kevin Bacon"})-[:ACTS_IN]->(m:Movie)<-[:ACTS_IN]-(a:Actor)
            MATCH (a:Actor)-[:ACTS_IN]->(m2:Movie)<-[:ACTS_IN]-(a2:Actor)
            WHERE NOT (kb)-[:ACTS_IN]->()<-[:ACTS_IN]-(a2) AND a2 <> kb 
            RETURN DISTINCT a2.name ORDER BY a2.name ASC
        """)
        return [(r[0]) for r in result]

if __name__ == "__main__":
    sol = Movie_queries("neo4jpass")
    print("---------- Q0 ----------")
    print(sol.q0())
    print("---------- Q1 ----------")
    print(sol.q1())
    print("---------- Q2 ----------")
    print(sol.q2())
    print("---------- Q3 ----------")
    print(sol.q3())
    print("---------- Q4 ----------")
    print(sol.q4())
    sol.transaction.close()
    sol.session.close()
    sol.driver.close()

