from neo4j import GraphDatabase

class Interface:
    def __init__(self, db_uri, db_user, db_pass):
        try:
            # Initialize the driver and verify connectivity
            self.driver = GraphDatabase.driver(db_uri, auth=(db_user, db_pass), encrypted=False)
            self.driver.verify_connectivity()
        except Exception as init_err:
            print("Error connecting to the database:", init_err)
            raise

    def close(self):
        try:
            self.driver.close()
        except Exception as close_err:
            print("Error closing the connection:", close_err)

    def create_projection(self):

        proj_query = """
            CALL gds.graph.project(
                'TripGraph',
                'Location',
                'TRIP',
                { relationshipProperties: 'distance' }
            )
        """
        try:
            with self.driver.session() as session:
                session.run(proj_query)
        except Exception as proj_err:
            print("Error while creating graph projection:", proj_err)
            raise

    def drop_projection(self):

        drop_query = "CALL gds.graph.drop('TripGraph') YIELD graphName;"
        try:
            with self.driver.session() as session:
                session.run(drop_query)
        except Exception as drop_err:
            print("Error while dropping graph projection:", drop_err)
            raise

    def bfs(self, start_loc, target_loc):

        try:
            self.create_projection()
            bfs_query = """
                MATCH (source:Location {name: $start_val}), (target:Location {name: $target_val})
                WITH id(source) AS source_id, [id(target)] AS target_ids
                CALL gds.bfs.stream('TripGraph', {
                    sourceNode: source_id,
                    targetNodes: target_ids
                })
                YIELD path
                RETURN path
            """
            params = {"start_val": start_loc, "target_val": target_loc}
            with self.driver.session() as session:
                result = session.run(bfs_query, params)
                results = result.data()
            self.drop_projection()
            return results
        except Exception as bfs_err:
            print("BFS execution error:", bfs_err)
            try:
                self.drop_projection()
            except Exception:
                pass
            return None

    def pagerank(self, max_iter, weight_prop):

        try:
            self.create_projection()
            pr_query = """
                CALL gds.pageRank.stream('TripGraph', {
                    maxIterations: $iter,
                    dampingFactor: 0.85,
                    relationshipWeightProperty: $weight
                })
                YIELD nodeId, score
                RETURN gds.util.asNode(nodeId).name AS name, score
                ORDER BY score DESC, name ASC
            """
            params = {"iter": max_iter, "weight": weight_prop}
            with self.driver.session() as session:
                result = session.run(pr_query, params)
                pr_results = result.data()
            self.drop_projection()
            if pr_results:
                # Return the node with maximum score and the node with minimum score
                highest = pr_results[0]
                lowest = pr_results[-1]
                return highest, lowest
            else:
                return None, None
        except Exception as pr_err:
            print("PageRank execution error:", pr_err)
            try:
                self.drop_projection()
            except Exception:
                pass
            return None, None
