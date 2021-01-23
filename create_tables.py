import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries, copy_table_queries, insert_table_queries


def drop_tables(cur, conn):
    """Drops existing tables, cur allows for SQL execution, conn is the connection to the datablse"""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

    """Creates new tables, cur allows for SQL execution, conn is the connection to the datablse"""
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Connects to AWS Redshift and creates new database then drops tables if they exist and creates new tables"""
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()