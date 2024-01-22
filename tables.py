import psycopg2
from queries import create_table_queries,drop_table_queries

def create_database():
    conn=psycopg2.connect(dbname="postgres",user="postgres",password=1234,host="localhost",port=5432)
    conn.set_session(autocommit=True)
    curr=conn.cursor()

    curr.execute("DROP DATABASE IF EXISTS dmodeling")
    curr.execute("CREATE DATABASE dmodeling with ENCODING 'utf-8' TEMPLATE template0")

    conn.close()

    conn=psycopg2.connect(dbname="dmodeling",user="postgres",password=1234,host="localhost",port=5432)
    curr=conn.cursor()

    return curr,conn

def drop_table(curr,conn):
    for query in drop_table_queries:
        curr.execute(query)
        conn.commit()

def create_table(curr,conn):
    for query in create_table_queries:
        curr.execute(query)
        conn.commit()


def main():
    curr,conn = create_database()


    create_table(curr,conn)
    print("Table creatd succesfully")

    curr.execute('select * from customer')

    conn.close()

if __name__ == "__main__":
    main()