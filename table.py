import psycopg2
import pandas as pd
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from metadata import get_metadata

metadata = get_metadata()

def get_connection():
    return psycopg2.connect(
        host='0.0.0.0',
        port=5432,
        database = metadata['db_name'],
        user="postgres",
        password="password1"
    )

def create_database():
    global metadata

    conn = psycopg2.connect(
        user='postgres',
        password='password1',
        host='0.0.0.0',
        port=5432
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    cursor = conn.cursor()

    sqlCreateDatabase = f"create database {metadata['db_name']};"
    cursor.execute(sqlCreateDatabase)

def create_table_query():
    global metadata
    query = f"create table {metadata['table_name']} ("
    for meta in metadata['col_metadata']:
        temp_query = f"{meta['name']} {meta['dtype']}"

        if meta['primary_key']:
            temp_query += 'PRIMARY KEY'
        temp_query += ","
        query += temp_query
    
    query = query[:-1]
    query += ");"
    return query


def create_table():
    global metadata
    conn = get_connection()
    cursor = conn.cursor()
    
    query = create_table_query()

    cursor.execute(query)
    conn.commit()
    conn.close()
    cursor.close()

def ingestion(df):
    global metadata

    conn = get_connection()

    tup = [tuple(x) for x in df.to_numpy()]
    columns = ','.join(list(df.columns))
    
    query  = f"INSERT INTO {metadata['table_name']}({columns}) VALUES (%s,%s,%s,%s,%s)"
    cursor = conn.cursor()
    
    try:
        import pdb; pdb.set_trace()
        cursor.executemany(query, tup)
        conn.commit()
    except:
        conn.rollback()
        cursor.close()
        print('No data ingested')
        return 1
    
    cursor.close()

def create_search_query(params):
    query = f"select * from {metadata['table_name']} "

    search = params['search'].split(' ')
    temp_query = 'where '
    tup = []
    for val in search:
        if val != "":
            temp_query += f"title like %s or description like %s or"
            tup.extend([val, val])
    
    if temp_query != 'where ':
        query += temp_query[:-2] + f"order by publish_time DESC LIMIT {params['results_per_page']} OFFSET {params['page_number']-1};"
    else:
        query +=  f"order by publish_time DESC LIMIT {params['results_per_page']} OFFSET {params['page_number']-1};"
    return query, tup

def get_df(params):
    global metadata
    conn = get_connection()
    cursor = conn.cursor()

    query, tup = create_search_query(params)
    
    if len(tup) >0:
        cursor.execute(query,(tup))
    else:
        cursor.execute(query)
    data = cursor.fetchall()

    df = pd.DataFrame(columns =[col['name'] for col in metadata['col_metadata']], data=data)
    # import pdb; pdb.set_trace()
    return df

if __name__ == '__main__':
    create_database()
    create_table()






