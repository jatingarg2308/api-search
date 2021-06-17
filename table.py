import psycopg2
import pandas as pd
import re
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from metadata import get_metadata

metadata = get_metadata()

def get_connection():
    return psycopg2.connect(
        host='localhost',
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
        host='localhost',
        port=5432
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    cursor = conn.cursor()

    sqlCreateDatabase = f"create database {metadata['db_name']};"
    try:
        print(f"Creating Database {metadata['db_name']}")
        cursor.execute(sqlCreateDatabase)
    except:
        print('Database already exists')

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
    try:
        print(f"Creating Table {metadata['table_name']}")
        cursor.execute(query)
        conn.commit()
    except:
        print('Table already exists')
    conn.close()
    cursor.close()

def ingestion(df):
    global metadata

    conn = get_connection()

    tup = [tuple(x) for x in df.to_numpy()]
    columns = ','.join(list(df.columns))
    primary_col = []
    for meta in metadata['col_metadata']:
        if meta['primary_key']:
            primary_col.append(meta['name'])
    
    primary = ','.join(primary_col)
    excluded = [f"EXCLUDED.{col}" for col in df.columns]
    exclude = ", ".join(excluded)
    
    query  = f"INSERT INTO {metadata['table_name']}({columns}) VALUES (%s,%s,%s,%s,%s) \
                ON CONFLICT ({primary}) DO UPDATE SET ({columns}) = ({exclude});"
    cursor = conn.cursor()
    
    try:
        print('Ingestion Data')
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
        val = re.sub("[^a-z0-9]", "", val)
        if val != "":
            temp_query += f"lower(title) like %s or lower(description) like %s or "
            tup.extend([f"%{val.lower()}%", f"%{val.lower()}%"])
    
    if temp_query != 'where ':
        query += temp_query[:-3] + f"order by publish_time DESC LIMIT {params['results_per_page']} OFFSET {params['page_number']-1};"
    else:
        query +=  f"order by publish_time DESC LIMIT {params['results_per_page']} OFFSET {params['page_number']-1}"
    return query, tup

def get_df(params):
    global metadata
    conn = get_connection()
    cursor = conn.cursor()

    query, tup = create_search_query(params)
    print('Fetching Data')
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






