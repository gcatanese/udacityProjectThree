import configparser
import psycopg2
from projectThree.sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load Staging tables from S3 files
    :param cur:
    :param conn:
    """
    print("Load staging tables")
    for query in copy_table_queries:
        print("copy cmd->", query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert into facts/dimensions tables (selecting & processing Staging tables)
    :param cur:
    :param conn:
    """
    print("Insert into tables")
    for query in insert_table_queries:
        print("copy cmd->", query)
        cur.execute(query)
        conn.commit()

def print_counters(cur):
    tables = ['staging_events', 'staging_songs', 'song_plays', 'users', 'songs', 'artists', 'time']
    for table in tables:
        cur.execute("SELECT COUNT(*) FROM " + table)
        result=cur.fetchone()
        print("Count for " + table, result[0])

def main():
    """
    Main method to start the ETL pipeline
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    print_counters(cur)

    conn.close()

if __name__ == "__main__":
    main()