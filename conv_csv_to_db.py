from sqlalchemy import  Integer, Float, String
from sqlalchemy import create_engine
import pandas as pd
import sqlite3
from sqlite3 import Error

def create_connection(db_file):
   conn = None
   try:
       conn = sqlite3.connect(db_file)
       print(f"Connected to {db_file}, sqlite version: {sqlite3.version}")
   except Error as e:
       print(e)
   finally:
       if conn:
           conn.close()

engine = create_engine('sqlite:///spdata.db', echo=True)


content = pd.read_csv('clean_stations.csv')
table_name = 'flight'
content.to_sql(
    table_name,
    engine,
    if_exists='replace',
    index=False,
    dtype={
        "station": Integer,
        "latitude": Float,
        "longitude": Float,
        "elevation": Float,
        "name": String,
        "country": String,
        "state": String,
    }
)

content = pd.read_csv('clean_measure.csv')
table_name = 'measure'
content.to_sql(
    table_name,
    engine,
    if_exists='replace',
    index=False,
    dtype={
        "station": Integer,
        "date": Integer,
        "precip": Float,
        "tobs": Integer,
    }
)
conn = create_connection("spdata.db")
def select(conn, table, **query):
    qs = []
    values = ()
    for k, v in query.items():
        qs.append(f"{k}=?")
        values += (v,)
    q = " AND ".join(qs)
    conn = create_connection("spdata.db")
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table} WHERE {q}", values)
    rows = cur.fetchall()
    return rows

select(conn,'flight', elevation=7)