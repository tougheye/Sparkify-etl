import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

# declaring functions to execute queries to drop tables
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

# declaring functions to execute queries to create staging and STAR Schema tables
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    print("Reading config...")
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    print("Connecting to Redshift...")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("Connected.")

    print("Dropping tables...")
    drop_tables(cur, conn)
    print("Tables dropped.")

    print("Creating tables...")
    create_tables(cur, conn)
    print("Tables created.")

    conn.close()
    print("Connection closed.")


if __name__ == "__main__":
    main()
