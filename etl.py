import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, drop_table_queries, create_table_queries


def load_staging_tables(cur, conn):
    """Load raw event and song data from S3 into Redshift staging tables

    Uses the Redshift COPY command. This can take a significant amount of
    time. Since data is processed in parallel increasing the number of nodes
    in the Redshift cluster can help to reduce the time taken.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Transform and load staging tables into fact and dimention tables"""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Entrypoint for the full ETL process"""

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
