import logging
from datetime import datetime

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json

def create_keyspace(session):
    # create keyspace here
    session.execute("""
    CREATE KEYSPACE IF NOT EXIST spark_stream
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """)

    print("keyspace created successfully !")
def create_table(session):
    # create
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
            id UUID PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            post_code TEXT,
            email TEXT,
            username TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT);
        """)

    print("Table created successfully!")

def insert_data(session, **kwargs):
    # insertion data here

    print("inserting data...")

    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        session.execute("""
                INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                    post_code, email, username, dob, registered_date, phone, picture)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (user_id, first_name, last_name, gender, address,
                  postcode, email, username, dob, registered_date, phone, picture))
        logging.info(f"Data inserted for {first_name} {last_name}")

    except Exception as e:
        logging.error(f'could not insert data due to {e}')

def create_spark_connection():
    # create spark connection
    s_connection = None
    try:
        s_connection = SparkSession.builder \
                        .appName("SparkDataStream") \
                        .config('spark.jars.pakages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                                        "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
                        .config('spark.cassandra.connection.host', 'localhost') \
                        .getOrCreate()

        s_connection.sparkContext.setLogLevel("ERROR")
        logging.info("spark connection created successfully")

    except Exception as e :
        logging.error(f"couln't create the spark session due to exception : {e}")

    return s_connection

def create_cassandra_connection():
    # creating cassandra connection
    try:
        cluster = Cluster(['localhost'])

        cas_session = cluster.connect()

        return cas_session

    except Exception as e:
        logging.error(f"could not create cassandra cluster due to {e}")
        return None
if __name__ == "__main__":
    spark_connection = create_spark_connection()

    if spark_connection is not None:
        session = create_cassandra_connection

        if session is not None:
            create_keyspace(session)
            create_table(session)