import urllib.parse
import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv
import urllib
import sqlalchemy 


abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)
load_dotenv()

DATABASE=os.getenv("DATABASE")
USER=os.getenv("USER")
HOST=os.getenv("HOST")
PASSWORD=os.getenv("PASSWORD")
PORT=os.getenv("PORT")

class DatabaseHandler:

    def __init__(self):
        self.__conn = psycopg2.connect(
            database=DATABASE,
            user=USER,
            host=HOST,
            password=PASSWORD,
            port=PORT,
        )
        db_string = "postgresql+psycopg2://%s:%s@%s:%s/%s" % (
            USER,
            urllib.parse.quote_plus(PASSWORD),
            HOST,
            PORT,
            DATABASE,
        )
        self.__db_engine = sqlalchemy.create_engine(db_string)
    
    def query(self, query: str) -> list:
        """Quering data

        Args:
            query (str): select query expected

        Returns:
            list: json data output
        """
        try:
            data = pd.read_sql(query, self.__conn)
            self.__conn.close()
            self.__db_engine.dispose()
            return data
        except Exception as e:
            print("Exception --->", e)

    def df_to_sql(self, data: pd.DataFrame, table_name: str, schema: str) -> bool:
        """Inserts data while also comparing for duplicacy

        Args:
            data (pd.DataFrame): dataframe which need to be pushed
            table_name (str): table where it needs to be pushed
            schema (str): schema where it needs to be pushed

        Returns:
            bool: true if success else false
        """
        try:
            # To make sure we are not pushing with same email again
            data.to_sql(
                table_name,
                self.__db_engine,
                schema=schema,
                if_exists="append",
                index=False,
            )
            self.__conn.close()
            self.__db_engine.dispose()
            return True
        except Exception as e:
            print("Exception ---->", e)
            return False