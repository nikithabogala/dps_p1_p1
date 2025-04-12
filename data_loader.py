import pyarrow.parquet as pq
import pandas as pd
from neo4j import GraphDatabase
import time
import os


class DataLoader:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=False)
        self.driver.verify_connectivity()


    def close(self):
        """
        Close the connection to the Neo4j database
        """
        self.driver.close()


    # Define a function to create nodes and relationships in the graph
    def load_transform_file(self, file_path):

        # Read the parquet file
        trips = pq.read_table(file_path)
        trips = trips.to_pandas()

        # Some data cleaning and filtering
        trips = trips[['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'trip_distance', 'fare_amount']]

        # Filter out trips that are not in bronx
        bronx = [3, 18, 20, 31, 32, 46, 47, 51, 58, 59, 60, 69, 78, 81, 94, 119, 126, 136, 147, 159, 167, 168, 169, 174, 182, 183, 184, 185, 199, 200, 208, 212, 213, 220, 235, 240, 241, 242, 247, 248, 250, 254, 259]
        trips = trips[trips.iloc[:, 2].isin(bronx) & trips.iloc[:, 3].isin(bronx)]
        trips = trips[trips['trip_distance'] > 0.1]
        trips = trips[trips['fare_amount'] > 2.5]

        # Convert date-time columns to supported format
        trips['tpep_pickup_datetime'] = pd.to_datetime(trips['tpep_pickup_datetime'], format='%Y-%m-%d %H:%M:%S')
        trips['tpep_dropoff_datetime'] = pd.to_datetime(trips['tpep_dropoff_datetime'], format='%Y-%m-%d %H:%M:%S')
        
        # Convert to csv and store in the current directory
        save_loc = "/var/lib/neo4j/import/" + file_path.split(".")[0] + '.csv'
        trips.to_csv(save_loc, index=False)

        # TODO: Your code here
        with self.driver.session() as session:
            print("Deleting existing nodes and relationships...")
            session.run("MATCH (n) DETACH DELETE n")
            print("Existing data deleted.")

        # Create constraints for faster lookup (optional but recommended)
        with self.driver.session() as session:
            print("Creating constraints...")
            try:
                session.run("CREATE CONSTRAINT location_name_unique IF NOT EXISTS FOR (l:Location) REQUIRE l.name IS UNIQUE")
                print("Constraint created successfully.")
            except Exception as e:
                print(f"Error creating constraint (might already exist): {e}")


        # Load data from CSV using Cypher
        # Note: Use the filename part of save_loc for LOAD CSV
        csv_filename = "file:///" + os.path.basename(save_loc) # Neo4j needs file:/// prefix for local files
        print(f"Loading data from: {csv_filename}")

        load_query = f"""
        LOAD CSV WITH HEADERS FROM '{csv_filename}' AS row
        // Ensure numeric types are correctly handled
        WITH row, toInteger(row.PULocationID) AS pickupID, toInteger(row.DOLocationID) AS dropoffID, \
            toFloat(row.trip_distance) AS distance, toFloat(row.fare_amount) AS fare, \
            datetime(replace(row.tpep_pickup_datetime, ' ', 'T')) AS pickup_dt, \
            datetime(replace(row.tpep_dropoff_datetime, ' ', 'T')) AS dropoff_dt

        // Create Pickup Location node if it doesn't exist
        MERGE (pickup:Location {{name: pickupID}})

        // Create Dropoff Location node if it doesn't exist
        MERGE (dropoff:Location {{name: dropoffID}})

        // Create TRIP relationship between Pickup and Dropoff nodes
        CREATE (pickup)-[:TRIP {{
            distance: distance,
            fare: fare,
            pickup_dt: pickup_dt,
            dropoff_dt: dropoff_dt
        }}]->(dropoff)
        """

        try:
            with self.driver.session() as session:
                print("Executing LOAD CSV query...")
                # Running the query
                session.run(load_query)
                print("Data loading query executed successfully.")
        except Exception as e:
            print(f"Error during data loading: {e}")


def main():

    total_attempts = 10
    attempt = 0

    # The database takes some time to starup!
    # Try to connect to the database 10 times
    while attempt < total_attempts:
        try:
            data_loader = DataLoader("neo4j://localhost:7687", "neo4j", "project1phase1")
            data_loader.load_transform_file("yellow_tripdata_2022-03.parquet")
            data_loader.close()
            
            attempt = total_attempts

        except Exception as e:
            print(f"(Attempt {attempt+1}/{total_attempts}) Error: ", e)
            attempt += 1
            time.sleep(10)


if __name__ == "__main__":
    main()

