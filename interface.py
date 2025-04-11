from neo4j import GraphDatabase
import neo4j.graph

class Interface:
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=False)
        self._driver.verify_connectivity()
    
    def _ensure_graph_projection(self, graph_name='myGraph', node_label='Location', rel_type='TRIP'):
        """Checks if graph projection exists, creates it if not."""
        try:
            with self._driver.session(database="neo4j") as session:
                 # Check if graph exists
                 exists_result = session.run(f"CALL gds.graph.exists('{graph_name}') YIELD exists RETURN exists").single()
                 if exists_result and exists_result['exists']:
                     print(f"Graph projection '{graph_name}' already exists.")
                     return

                 print(f"Creating graph projection '{graph_name}'...")
                 project_query = f"""
                    CALL gds.graph.project(
                        '{graph_name}',
                        '{node_label}',
                        {{
                            {rel_type}: {{
                                properties: ['distance', 'fare']
                            }}
                        }}
                    ) YIELD graphName, nodeCount, relationshipCount
                    RETURN graphName, nodeCount, relationshipCount
                 """

                 result = session.run(project_query).single()
                 if result:
                     print(f"Graph projection '{result['graphName']}' created with {result['nodeCount']} nodes and {result['relationshipCount']} relationships.")
                 else:
                     print(f"Warning: Failed to create graph projection '{graph_name}'.")

        except Exception as e:
            # Attempt to drop and recreate if projection failed due to incompatible schema
            if "project" in str(e).lower():
                 try:
                    with self._driver.session(database="neo4j") as session:
                        print(f"Dropping potentially incompatible graph '{graph_name}' due to error: {e}")
                        session.run(f"CALL gds.graph.drop('{graph_name}', false) YIELD graphName;")
                        # Retry projection
                        print(f"Retrying graph projection '{graph_name}'...")
                        result = session.run(project_query).single()
                        if result:
                            print(f"Graph projection '{result['graphName']}' created successfully on retry.")
                        else:
                             print(f"Warning: Failed to create graph projection '{graph_name}' on retry.")
                 except Exception as drop_e:
                     print(f"Error creating/dropping graph projection '{graph_name}': {drop_e}")
            else:
                print(f"Error ensuring graph projection '{graph_name}': {e}. Ensure GDS is installed and schema matches.")


    def close(self):
        self._driver.close()

    def bfs(self, start_node, last_node):
        # TODO: Implement this method

        # Ensure graph projection exists
        self._ensure_graph_projection() # Assumes projection named 'myGraph'

        query = """
            MATCH (start:Location {name: $start_node_param}), (end:Location {name: $last_node_param})
            CALL gds.bfs.stream(
                'myGraph',
                {
                    sourceNode: id(start),
                    targetNodes: [id(end)]
                }
            )
            YIELD path
            RETURN path
        """
        try:
            with self._driver.session(database="neo4j") as session:
                result = session.run(query, start_node_param=start_node, last_node_param=last_node)
                path_record = result.single() # Get the single path result record

                if path_record and path_record['path']:
                    # Process the path object into the desired format
                    # The path object contains neo4j.graph.Node objects
                    raw_path = path_record['path']
                    processed_path = []
                    # Path object alternates nodes and relationships: node1, rel1, node2, rel2, ... nodeN
                    for item in raw_path:
                         # Check if the item is a Node object
                         if isinstance(item, neo4j.graph.Node):
                              # Extract properties, specifically 'name'
                              node_properties = dict(item.items())
                              processed_path.append({'name': node_properties.get('name')}) # Create dict with 'name' key

                    # Return the format expected by the tester: list containing one dict
                    return [{'path': processed_path}]
                else:
                    print(f"BFS: No path found between {start_node} and {last_node}.")
                    return [] # Return empty list if no path found, as tester might expect list
        except Exception as e:
            print(f"Error running BFS query: {e}")
            return [] # Return empty list on error
        # raise NotImplementedError

    def pagerank(self, max_iterations, weight_property):
        # TODO: Implement this method
        """
        Runs the PageRank algorithm using GDS write mode.
        Returns the nodes with max and min scores in the format expected by the tester.
        """
         # Ensure graph projection exists
        self._ensure_graph_projection() # Assumes projection named 'myGraph'

        # Ensure the specific weight property exists in the projection if provided
        if weight_property:
            try:
                 with self._driver.session(database="neo4j") as session:

                    print(f"Ensuring graph projection includes property: {weight_property}")
                    session.run(f"""
                        CALL gds.graph.project(
                            'myGraph_temp_pagerank', 'Location',
                            {{ TRIP: {{ properties: ['{weight_property}'] }} }}
                        ) YIELD graphName
                        CALL gds.graph.drop('myGraph_temp_pagerank', false) YIELD graphName AS droppedGraph
                        RETURN 'Property check simulated by attempting projection'
                    """) # This doesn't actually change 'myGraph', just checks if projection is possible
            except Exception as e:
                print(f"Warning: Could not verify weight property '{weight_property}' in projection or projection failed: {e}. PageRank might fail.")



        # Configure PageRank write mode
        pagerank_config = {
            'maxIterations': max_iterations,
            'dampingFactor': 0.85,
            'writeProperty': 'pagerankScore' # Use a distinct property name
        }
        if weight_property:
            pagerank_config['relationshipWeightProperty'] = weight_property
            print(f"Running PageRank with weight property: {weight_property}")
        else:
            print("Running unweighted PageRank")

        write_query = """
            CALL gds.pageRank.write('myGraph', $config)
            YIELD nodePropertiesWritten, ranIterations
            RETURN nodePropertiesWritten, ranIterations
        """

        # Query to get min and max score nodes AFTER writing
        query_min_max = """
            MATCH (n:Location)
            WHERE n.pagerankScore IS NOT NULL
            WITH min(n.pagerankScore) AS minScore, max(n.pagerankScore) AS maxScore
            // Find one node for min score and one for max score
            MATCH (minNode:Location {pagerankScore: minScore})
            WITH minNode, maxScore
            LIMIT 1 // Ensure only one min node if multiple have the same score
            MATCH (maxNode:Location {pagerankScore: maxScore})
            WITH minNode, maxNode
            LIMIT 1 // Ensure only one max node
            RETURN
                {name: maxNode.name, score: maxNode.pagerankScore} AS maxResult,
                {name: minNode.name, score: minNode.pagerankScore} AS minResult
        """

        try:
            with self._driver.session(database="neo4j") as session:
                # Run PageRank write
                write_result = session.run(write_query, config=pagerank_config).single()
                if not write_result or write_result['nodePropertiesWritten'] == 0:
                     print("PageRank write did not run or write any properties.")
                     # Clean up potentially written property if needed
                     session.run("MATCH (n:Location) WHERE n.pagerankScore IS NOT NULL REMOVE n.pagerankScore")
                     return [] # Return empty list or appropriate error indicator

                print(f"PageRank ran for {write_result['ranIterations']} iterations, wrote {write_result['nodePropertiesWritten']} properties.")

                # Query for min and max score nodes
                min_max_result = session.run(query_min_max).single()

                # Clean up the written property
                session.run("MATCH (n:Location) WHERE n.pagerankScore IS NOT NULL REMOVE n.pagerankScore")
                print("Cleaned up temporary PageRank scores.")

                if min_max_result:
                    # Return in the list format [max_result, min_result] expected by tester
                    return [min_max_result['maxResult'], min_max_result['minResult']]
                else:
                    print("Could not retrieve min/max PageRank scores after write.")
                    return []

        except Exception as e:
            print(f"Error running PageRank query: {e}")
            # Attempt cleanup on error too
            try:
                with self._driver.session(database="neo4j") as session:
                     session.run("MATCH (n:Location) WHERE n.pagerankScore IS NOT NULL REMOVE n.pagerankScore")
            except:
                pass # Ignore cleanup error
            return [] # Return empty list on error
        # raise NotImplementedError

