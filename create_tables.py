import configparser
import platform
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def create_tables(cur, conn):
    for query in create_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()

def drop_tables(cur, conn):
    for query in drop_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()

def main():
    config = configparser.ConfigParser()
    read_file_name = 'dwh.cfg'
    if platform.system() == 'Windows':
        read_file_name = 'D:/AWS/'+read_file_name
    config.read_file(open(read_file_name))

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()