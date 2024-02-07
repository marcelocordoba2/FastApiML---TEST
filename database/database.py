import os
import pandas as pd
from sqlalchemy import create_engine,text
from sqlalchemy import create_engine
import psycopg2
from psycopg2.extras import DictCursor
from pandas import json_normalize
from sqlalchemy import create_engine,text
from pandas import json_normalize
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import logging
import json
import traceback

def conexion():
    try:
        with open('db.json', 'r') as archivo_json:
            conn_str = json.load(archivo_json)
            conn_str=conn_str['conn_str']
    except FileNotFoundError:
        conn_str = os.getenv("DATABASE_URL")
    return conn_str