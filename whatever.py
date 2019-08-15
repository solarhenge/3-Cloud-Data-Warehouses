from sql_queries import copy_table_queries, update_table_queries, insert_table_queries, counting_stars_queries
import configparser
import platform
import psycopg2

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()

def update_tables(cur, conn):
    for query in update_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()

def counting_stars(cur, conn):
    for query in counting_stars_queries:
        print(query)
        cur.execute(query)
        print(cur.fetchone()[0])

def main():
    config = configparser.ConfigParser()
    read_file_name = 'dwh.cfg'
    if platform.system() == 'Windows':
        read_file_name = 'D:/AWS/'+read_file_name
    config.read_file(open(read_file_name))

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    update_tables(cur, conn)
    insert_tables(cur, conn)
    counting_stars(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()