from datetime import datetime
from etl_processor import ETLProcessor
import pandas as pd
import numpy as np
import pytest


class TestMain():

    def test_class_initialization(self):
        df = pd.read_json("test.json")
        e = ETLProcessor(df)
        assert e.df.equals(df)

        # CASE 2: Testing the json input
        test = [
            {
                "id": 1,
                "salary": "200"
            }
        ]
        testdf = pd.json_normalize(test)
        e = ETLProcessor(test)
        assert e.df.equals(testdf)

    def test_read_data(self,mocker):
        test = [
            {
                "id": 1,
                "salary": "200"
            }
        ]
        mock_another_instance = mocker.Mock()
        mock_another_instance.query.return_value = pd.DataFrame({"name": ["apple"]})
        mocker.patch('etl_processor.DatabaseHandler', return_value=mock_another_instance)
        e = ETLProcessor(test)
        e.read_data()
        mock_another_instance.query.assert_called_once_with(
            "select * from tmp.employees_raw"
        )


        mock_another_instance = mocker.Mock()
        mock_another_instance.query.return_value = pd.DataFrame()
        mocker.patch('etl_processor.DatabaseHandler', return_value=mock_another_instance)
        e1 = ETLProcessor(test)
        e1.read_data()


    def test_check_name(self):
        # CASE 1: Testing one valid
        test = [
            {
                "id": 1,
                "name": "Test name"
            }
        ]
        df = pd.json_normalize(test)
        e = ETLProcessor(df)
        e.remove_empty_name()
        assert (e.df.equals(df))

        # CASE 2: Testing empty Name
        test = [
            {
                "id": 1,
                "name": ""
            }
        ]
        df = pd.json_normalize(test)
        e = ETLProcessor(df)
        e.remove_empty_name()
        assert (e.df.empty)

        # CASE 3: Checking None value
        test = [
            {
                "name": None
            }
        ]
        df = pd.json_normalize(test)
        e = ETLProcessor(df)
        e.remove_empty_name()
        assert (e.df.empty)
    
    def test_remove_invalid_email(self):
        # CASE 1: Checking correct value
        test = [
            {
                "id": 1,
                "email": "apple.me@gmail.com"
            }
        ]
        df = pd.json_normalize(test)
        e = ETLProcessor(df)
        e.remove_invalid_email()
        assert (e.df.equals(df))

        # CASE 2: Checking incorrect values
        test = [
            {
                "id": 1,
                "email": "apple.megmail.com"
            },
            {
                "id": 2,
                "email": "apple.me@gmail."
            },
            {
                "id": 3,
                "email": "apple.me@gmailcom"
            },
            {
                "id": 4,
                "email": "apple.megmail.com"
            },
            {
                "id": 5,
                "email": "@gmail.com"
            },
            {
                "id": 6,
                "email": "applemegmailcom"
            }
        ]
        df = pd.json_normalize(test)
        e = ETLProcessor(df)
        e.remove_invalid_email()
        assert (e.df.empty)
    
        # CASE 3: Checking None value
        test = [
            {
                "email": None
            }
        ]
        df = pd.json_normalize(test)
        e = ETLProcessor(df)
        e.remove_invalid_email()
        assert (e.df.empty)
    
    def test_remove_invalid_salary(self):
        # CASE 1: Checking correct value
        test = [
            {
                "id": 1,
                "salary": "200"
            },
            {
                "id": 2,
                "salary": "100"
            }
        ]
        testdf = pd.json_normalize(test)
        assertdf = pd.DataFrame()
        assertdf["id"] = [1,2]
        assertdf["salary"] = np.array([200,100])
        e = ETLProcessor(testdf)
        e.remove_nega_sal()
        assert e.df.equals(assertdf)

        # CASE 2: Checking negative value
        test = [
            {
                "id": 1,
                "salary": "-200000"
            }
        ]
        testdf = pd.json_normalize(test)
        assertdf = pd.DataFrame()
        assertdf["id"] = [1]
        assertdf["salary"] = np.array([200000])
        e = ETLProcessor(testdf)
        e.remove_nega_sal()
        assert e.df.equals(assertdf)

        # Case 3: Checking non digit/digit mixed values
        test = [
            {
                "id": 1,
                "salary": "200UST"
            },
            {
                "id": 2,
                "salary": "UST100"
            }
        ]
        testdf = pd.json_normalize(test)
        e = ETLProcessor(testdf)
        e.remove_nega_sal()
        assertdf = pd.DataFrame()
        assertdf["id"] = [1,2]
        assertdf["salary"] = np.array([200,100])
        e = ETLProcessor(testdf)
        e.remove_nega_sal()
        assert e.df.equals(assertdf)

        # CASE 4: checking non-digit values
        test = [
            {
                "id": 1,
                "salary": "asdasd"
            }
        ]
        testdf = pd.json_normalize(test)
        e = ETLProcessor(testdf)
        e.remove_nega_sal()
        assert e.df.empty
    
        # CASE 5: checking None values
        test = [
            {
                "id": 1,
                "salary": None
            }
        ]
        testdf = pd.json_normalize(test)
        e = ETLProcessor(testdf)
        e.remove_nega_sal()
        assert e.df.empty

    def test_check_date(self):
        # CASE 1: checking Correct values
        test = [
            {
                "id": 1,
                "join_date": "2022-02-20"
            }
        ]
        testdf = pd.json_normalize(test)
        e = ETLProcessor(testdf)
        e.remove_invalid_date()
        assertdf = pd.DataFrame()
        assertdf["id"] = [1]
        assertdf["join_date"] = np.array([datetime(2022, 2, 20)])
        assert e.df.equals(assertdf)

        # CASE 2: Checking Different date format
        test = [
            {
                "join_date": "2022/02/20"
            },
            {
                "join_date": "2022.02.20"
            },
            {
                "join_date": "2022-02-20"
            },
            {
                "join_date": "20-02-2022"
            },
            {
                "join_date": "20/02/2022"
            },
            {
                "join_date": "20.02.2022"
            },
            {
                "join_date": "02-20-2022"
            },
            {
                "join_date": "02/20/2022"
            },
            {
                "join_date": "02.20.2022"
            },
        ]
        testdf = pd.json_normalize(test)
        e = ETLProcessor(testdf)
        e.remove_invalid_date()
        assertdf = pd.DataFrame()
        assertdf["join_date"] = np.array([
            datetime(2022, 2, 20),
            datetime(2022, 2, 20),
            datetime(2022, 2, 20),
            datetime(2022, 2, 20),
            datetime(2022, 2, 20),
            datetime(2022, 2, 20),
            datetime(2022, 2, 20),
            datetime(2022, 2, 20),
            datetime(2022, 2, 20),
        ])
        assert e.df.equals(assertdf)

        # CASE 3: Checking None value
        test = [
            {
                "join_date": None
            }
        ]
        testdf = pd.json_normalize(test)
        e = ETLProcessor(testdf)
        e.remove_invalid_date()
        assert e.df.empty

        # CASE 4: Test absolutely not date
        test = [
            {
                "join_date": "2022-02-30"
            }
        ]
        testdf = pd.json_normalize(test)
        e = ETLProcessor(testdf)
        e.remove_invalid_date()
        assert e.df.empty
    
    def test_transform_data(self):
        test = [
            {
                "id": 1,
                "name": "",
                "email": "john.doe@example.com",
                "salary": "50000",
                "department": "HR",
                "join_date": "2022-01-10"
            },
            {
                "id": 2,
                "name": "Jane Doe",
                "email": "invalid_email",
                "salary": "60000",
                "department": "Marketing",
                "join_date": "2022-05-15"
            }
        ]
        e = ETLProcessor(test)
        e.transform_data()
        assert e.df.empty

    def test_load_data(self,mocker):
        test = [
            {
                "salary": "200"
            }
        ]
        mock_another_instance = mocker.Mock()
        mock_another_instance.df_to_sql.return_value = True
        mocker.patch('etl_processor.DatabaseHandler', return_value=mock_another_instance)
        e = ETLProcessor(test)
        e.load_data()
        mock_another_instance.df_to_sql.assert_called_once()

        # CASE 2: Testing Exception
        mock_another_instance = mocker.Mock()
        mock_another_instance.df_to_sql.return_value = False
        mocker.patch('etl_processor.DatabaseHandler', return_value=mock_another_instance)
        e = ETLProcessor(test)
        e.load_data()
        mock_another_instance.df_to_sql.assert_called_once()
    
    def test_load_outlier_data(self,mocker):
        test = [
            {
                "salary": "200"
            }
        ]
        mock_another_instance = mocker.Mock()
        mock_another_instance.df_to_sql.return_value = True
        mocker.patch('etl_processor.DatabaseHandler', return_value=mock_another_instance)
        e = ETLProcessor(test)
        e.load_outlier_data()
        mock_another_instance.df_to_sql.assert_called_once()

        # CASE 2: Testing Exception
        mock_another_instance = mocker.Mock()
        mock_another_instance.df_to_sql.return_value = False
        mocker.patch('etl_processor.DatabaseHandler', return_value=mock_another_instance)
        e = ETLProcessor(test)
        e.load_outlier_data()
        mock_another_instance.df_to_sql.assert_called_once()