import os
from database_handler import DatabaseHandler
from dotenv import load_dotenv
import pandas as pd
import pytest
import urllib
import sqlalchemy


load_dotenv()
DATABASE = os.getenv("DATABASE")
USER = os.getenv('USER')
HOST = os.getenv('HOST')
PASSWORD = os.getenv('PASSWORD')
PORT =  os.getenv('PORT')


class Any:
    def __eq__(self, other):
        return True

ANY = Any()

class TestMain():

    def test_connection_mock(self, mocker):
        mock_file = mocker.mock_open()
        mocker.patch('psycopg2.connect', mock_file)
        mocker.patch('urllib.parse.quote_plus')
        mocker.patch('sqlalchemy.create_engine')
        d = DatabaseHandler()
        mock_file.assert_called_once_with(
            database =DATABASE,
            user =USER,
            host =HOST,
            password =PASSWORD,
            port =PORT
        )
        urllib.parse.quote_plus.assert_called_once_with(
            PASSWORD
        )
        db_string = "postgresql+psycopg2://%s:%s@%s:%s/%s" % (
            USER,
            urllib.parse.quote_plus(PASSWORD),
            HOST,
            PORT,
            DATABASE,
        )
        sqlalchemy.create_engine.assert_called_once_with(
            db_string
        )
    
    def test_read_mock(self, mocker):
        mock_file = mocker.mock_open()
        mocker.patch.object(DatabaseHandler, "__init__", return_value=None)
        mocker.patch('pandas.read_sql', mock_file)
        db = DatabaseHandler.__new__(DatabaseHandler)
        mock_db_engine = mocker.Mock()
        mock_db_conn = mocker.Mock()
        mock_db_engine.dispose.return_value = None
        mock_db_conn.close.return_value = None
        db._DatabaseHandler__conn = mock_db_conn
        db._DatabaseHandler__db_engine = mock_db_engine
        db.query("SELECT * from tmp.employees_raw")
        mock_file.assert_called_once_with(
            "SELECT * from tmp.employees_raw",
            ANY
        )
        mock_db_conn.close.assert_called_once()
        mock_db_engine.dispose.assert_called_once()

        # TESTING EXCEPTION
        with pytest.raises(Exception):
            mock_file = mocker.mock_open()
            mocker.patch.object(DatabaseHandler, "__init__", return_value=None)
            mocker.patch('pandas.read_sql', mock_file)
            db = DatabaseHandler.__new__(DatabaseHandler)
            mock_db_engine = mocker.Mock()
            mock_db_conn = mocker.Mock()
            mock_db_engine.dispose.return_value = None
            mock_db_conn.close.return_value = None
            db._DatabaseHandler__conn = None
            db._DatabaseHandler__db_engine = mock_db_engine
            db.query(None)
            mock_file.assert_called_once_with(
                "SELECT * from tmp.employees_raw",
                ANY
            )

    def test_write_mock(self, mocker):
        mock_file = mocker.mock_open()
        mocker.patch.object(DatabaseHandler, "__init__", return_value=None)
        db = DatabaseHandler()
        mock_db_engine = mocker.Mock()
        mock_db_conn = mocker.Mock()
        mock_db_engine.dispose.return_value = None
        mock_db_conn.close.return_value = None
        db._DatabaseHandler__conn = mock_db_conn
        db._DatabaseHandler__db_engine = mock_db_engine
        mocker.patch('pandas.DataFrame.to_sql', mock_file)
        data = pd.DataFrame({"name": ["a","b"]})
        db.df_to_sql(data, "test_table","test_schema")
        mock_file.assert_called_once_with(
            "test_table",
            ANY,
            schema = "test_schema",
            if_exists="append",
            index=False,
        )

        # # TESTING EXCEPTION
        with pytest.raises(Exception):
            mock_file = mocker.mock_open()
            mocker.patch.object(DatabaseHandler, "__init__", return_value=None)
            db = DatabaseHandler()
            mock_db_engine = mocker.Mock()
            mock_db_conn = mocker.Mock()
            mock_db_engine.dispose.return_value = None
            mock_db_conn.close.return_value = None
            db._DatabaseHandler__conn = None
            db._DatabaseHandler__db_engine = None
            mocker.patch('pandas.DataFrame.to_sql', mock_file)
            data = pd.DataFrame({"name": ["a","b"]})
            db.df_to_sql(data, "test_table","test_schema")
            mock_file.assert_called_once_with(
                None,
                ANY,
                schema = "test_schema",
                if_exists="append",
                index=False,
            )
