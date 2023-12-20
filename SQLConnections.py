import pandas as pd
import logging

from typing import Tuple, Union
from sqlalchemy import create_engine
from sqlalchemy import insert
logger = logging.getLogger()
import psycopg2


class DataBaseManager:
    def __init__(self, sql_alchemy_connection: str):
        # sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@postgres-db:5432/airflow_db
        self.user = sql_alchemy_connection.split('//')[1].split(':')[0]
        self.password = sql_alchemy_connection.split('//')[1].split(':')[1].split('@')[0]
        self.host = sql_alchemy_connection.split('//')[1].split(':')[1].split('@')[1]
        self.port = sql_alchemy_connection.split('//')[1].split(':')[2].split('/')[0]
        self.db = sql_alchemy_connection.split('//')[1].split(':')[2].split('/')[1]
        self.logger = logging.getLogger()

    def insert_df_to_database(self, df: pd.DataFrame, schema: str, table: str) -> pd.DataFrame:
        """ Lectura de una tabla a través de una query"""
        engine = create_engine(f'postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}')
        with engine.connect() as connection:
            rows = df.to_sql(table, engine, schema=schema, if_exists='append', index=False)
            if rows is None:
                self.logger = logging.getLogger(f"Problema al escribir rows en base de dato")
            else:
                self.logger = logging.getLogger(f"Escritura en base de datos realizada nº total: {rows}")
        engine.dispose()

    def upsert_df_to_database(self, delete_query: str, df: pd.DataFrame, schema: str, table: str) -> None:
        response = ''
        """ Upsert mediante borrado e insercion"""
        engine = create_engine(f'postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}')
        with engine.connect() as connection:
            # borrar filas segun el criterio del algoritmo
            engine.execute(delete_query)
            # insertar
            rows = df.to_sql(table, engine, schema=schema, if_exists='append', index=False)
            if rows is None:
                logging.info(f"Problema al escribir rows en base de dato")
                response = 'Bad insert'
            else:
                logging.info(f"Escritura en base de datos realizada nº total: {rows} en id: {df['id'][0]}")
                response = 'Good insert'
        engine.dispose()
        return response

    def fetch_data(self, query: str) -> pd.DataFrame:
        """ Coge datos desde una base de datos especificada una query"""
        engine = create_engine(f'postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}')
        with engine.connect() as connection:
            df = pd.read_sql(query, connection)

        self.logger = logging.getLogger(f"Lectura de {len(df)}")
        engine.dispose()
        return df



    def execute_query(self, query: str):
        """Execute any query"""
        response = []
        engine = create_engine(f'postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}')
        with engine.connect() as connection:
            if 'SELECT' == query[:6].upper():
                df = pd.read_sql(query, connection)
            else:
                engine.execute(query)
                df = None
        engine.dispose()
        return df

