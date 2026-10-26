
import json
from collections import Counter, deque
from kafka import KafkaConsumer
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich.console import Console, Group
from pymongo import MongoClient
import redis
from cassandra.cluster import Cluster
from neo4j import GraphDatabase
console = Console()
# MongoDB connection
mongo_client = MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["crime_platform"]
events_collection = mongo_db["crime_events"]

# Redis connection
redis_client = redis.Redis(
    host="localhost",
    port=6379,
    decode_responses=True
)

# Neo4j connection
neo4j_driver = GraphDatabase.driver(
    "bolt://localhost:7687",
    auth=("neo4j", "crimepassword123")
)
# Cassandra connection
cassandra_cluster = Cluster(["localhost"])
cassandra_session = cassandra_cluster.connect()

cassandra_session.execute("""
    CREATE KEYSPACE IF NOT EXISTS crime_platform
    WITH replication = {
        'class': 'SimpleStrategy',
        'replication_factor': 1
    }
""")

cassandra_session.set_keyspace("crime_platform")

cassandra_session.execute("""
    CREATE TABLE IF NOT EXISTS crime_events (
        event_id text PRIMARY KEY,
        timestamp text,
        crime_type text,
        location text,
        source text
    )
""")
consumer = KafkaConsumer(
    "crime-events",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="latest",
    group_id="crime-dashboard",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

crime_counts = Counter()
location_counts = Counter()
relationship_counts = Counter()
recent_events = deque(maxlen=10)
total = 0


def make_dashboard():
    summary = Table(title="LIVE STREAM SUMMARY")
    summary.add_column("Metric")
    summary.add_column("Count")
    summary.add_row("Total events", str(total))
    summary.add_row("Crime types", str(len(crime_counts)))
    summary.add_row("Locations", str(len(location_counts)))

    crimes = Table(title="Crime Type Analysis")
    crimes.add_column("Crime")
    crimes.add_column("Count")
    for name, count in crime_counts.most_common():
        crimes.add_row(name, str(count))

    areas = Table(title="Location Analysis")
    areas.add_column("Location")
    areas.add_column("Count")
    for name, count in location_counts.most_common():
        areas.add_row(name, str(count))
        
    relationships = Table(title="Crime Type vs Location")
    relationships.add_column("Crime Type")
    relationships.add_column("Location")
    relationships.add_column("Count", justify="right")

    for (crime, area), count in relationship_counts.most_common():
        relationships.add_row(crime, area, str(count))

    recent = Table(title="Latest Incoming Events")
    recent.add_column("Time")
    recent.add_column("Crime")
    recent.add_column("Location")
    for event in reversed(recent_events):
        recent.add_row(
            event["timestamp"][11:19],
            event["crime_type"],
            event["location"],
        )

    
    return Panel(
        Group(summary, crimes, areas, relationships, recent),
        title="CRIME PATTERN & RELATIONSHIP ANALYSIS",
        subtitle="Kafka stream | Simulated data",
    )

print("Waiting for events...")


try:
    with Live(make_dashboard(), console=console, refresh_per_second=4) as live:
        for message in consumer:
            event = message.value

            # Save event in MongoDB
            events_collection.update_one(
                {"event_id": event["event_id"]},
                {"$set": event},
                upsert=True
            )
            # Redis
            redis_client.incr("total_events")
            redis_client.incr(f"crime:{event['crime_type']}")
            redis_client.incr(f"location:{event['location']}")

            # Save event in Cassandra
            cassandra_session.execute(
                """
                INSERT INTO crime_events
                (event_id, timestamp, crime_type, location, source)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    event["event_id"],
                    event["timestamp"],
                    event["crime_type"],
                    event["location"],
                    event["source"]
                )
            )
            # Neo4j
            with neo4j_driver.session() as session:
                session.run(
                    """
                    MERGE (c:Crime {crime_type: $crime_type})
                    MERGE (l:Location {name: $location})
                    MERGE (c)-[:OCCURS_AT]->(l)
                    """,
                    crime_type=event["crime_type"],
                    location=event["location"]
                )

            # Dashboard counters â€” outside Neo4j with block
            total += 1
            crime_counts[event["crime_type"]] += 1
            location_counts[event["location"]] += 1
            relationship_counts[
                (event["crime_type"], event["location"])
            ] += 1

            recent_events.append(event)
            live.update(make_dashboard())

except KeyboardInterrupt:
    print("Dashboard stopped.")

finally:
    consumer.close()
