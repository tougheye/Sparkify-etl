import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

# declaring the function to run queries to load the staging tables
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print(f"Executing query: {query}")
        cur.execute(query)
        conn.commit()
   

# declaring the function to run queries to load the Fact and Dimension tables
def insert_tables(cur, conn):
    for query in insert_table_queries:
        print(f"Executing query: {query}")
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
