from database_handler import DatabaseHandler
import pandas as pd
import numpy as np
import json
import os
import re
import datetime

abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

class ETLProcessor:
    """Etl job to extract, transform and load a set of data from one table in postgres to another
    """
    def __init__(self, data: list|pd.DataFrame|None=None):
        """Sets enviroment variables

        Args:
            data (list | None, optional): Pandas dataframe object. Defaults to None.
        """
        self.df = None
        if isinstance(data,list):
            self.df = pd.json_normalize(data)
        if isinstance(data, pd.DataFrame):
            self.df = data
        self.copy_df = None
        if self.df is not None:
            self.copy_df = self.df.copy()
            self.copy_df["reason"] = ""

    def read_data(self) -> list | None:
        """Read SQL data to Dataframe

        Returns:
            list | None: Table data in json or None if error
        """
        try:
            db = DatabaseHandler()
            self.df = db.query("select * from tmp.employees_raw")
            self.copy_df = self.df.copy()
            self.copy_df["reason"] = ""
            if self.df.empty:
                raise Exception("Empty table")
            return json.loads(self.df.to_json(orient="records"))
        except Exception as e:
            print("Exception ---->", e)
            return None

    def remove_empty_name(self) -> list | None:
        """Removes empty name

        Returns:
            list | None: Table data in json or None if error
        """
        try:
            self.df = self.df[self.df.name != ""]
            self.df = self.df.dropna(subset=["name"])

            # Capturing the exception cases with reason
            mask = (~self.copy_df["id"].isin(self.df["id"])) & (self.copy_df['reason'] == "")
            self.copy_df.loc[mask, "reason"] += self.copy_df.loc[mask, "reason"].astype(str) + "Empty Name"
            return json.loads(self.df.to_json(orient="records"))
        except Exception as e:
            print("Exception ---->", e)
            return None

    def remove_invalid_email(self) -> list | None:
        """Remove invalid email and duplicate emails

        Returns:
            list | None: Table data in json or None if error
        """
        regex = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b"
        def check_email(x: str) -> bool:
            """Checks a string if it is passing email logic using regex

            Args:
                x (str): Email to be tested

            Returns:
                bool: True if email, else False
            """
            if x:
                return re.fullmatch(regex, x) is not None
            return False
        try:
            check_email_vectorize = np.vectorize(check_email)
            self.df = self.df[
                check_email_vectorize(self.df.email)
            ]
            self.df = self.df.dropna(subset=["email"])
            self.df = self.df.drop_duplicates(subset=["email"])
            
            # Capturing the exception cases with reason
            mask = (~self.copy_df["id"].isin(self.df["id"])) & (self.copy_df['reason'] == "")
            self.copy_df.loc[mask, "reason"] += self.copy_df.loc[mask, "reason"].astype(str) + "Invalid or Duplicated Email"
            return json.loads(self.df.to_json(orient="records"))
        except Exception as e:
            print("Exception ---->", e)
            return None

    def remove_nega_sal(self) -> list | None:
        """Fetches only numbers from salary field, also removes negative or 0 salary

        Returns:
            list | None: Table data in json or None if error
        """
        def clean_sal(x: str|None) -> int:
            """Cleaning the salary string by extracting just the number

            Args:
                x (str): string which needs cleaning

            Returns:
                int: salary if there's a number, or 0
            """
            if x:
                return int(re.sub(r"[^\d]+", "", x) or "0")
            return 0
        
        try:
            clean_sal_vectorize = np.vectorize(clean_sal)
            self.df.salary = clean_sal_vectorize(self.df.salary)
            self.df = self.df[self.df.salary > 0]
            self.df = self.df.dropna(subset=["salary"])

            # Capturing the exception cases with reason
            mask = (~self.copy_df["id"].isin(self.df["id"])) & (self.copy_df['reason'] == "")
            self.copy_df.loc[mask, "reason"] += self.copy_df.loc[mask, "reason"].astype(str) + "Invalid Salary or less than 0 value"
            return json.loads(self.df.to_json(orient="records"))
        except Exception as e:
            print("Exception ---->", e)
            return None

    def remove_invalid_date(self) -> list | None:
        """Removes invalid date using the function

        Returns:
            list | None: Table data in json or None if error
        """

        def rectify_date(d: str | None) -> datetime.datetime | None:
            """This function converts any format of datetime consisting of yyyy, mm, dd into proper universal yyyy-mm-dd

            Args:
                d (str): the string which needs to be rectify

            Returns:
                datetime.datetime | None: Datetime parsed object or None if error
            """
            if not d:
                return None
            sep = re.findall(r"[^0-9]", d)
            if sep:
                t = d.split(sep[0])
                y = 0
                m = 0
                dd = 0
                for i in t:
                    if len(i) == 4 and y == 0:
                        y = int(i)
                    elif int(i) < 13 and m == 0:
                        m = int(i)
                    else:
                        dd = int(i)
                # print(y,m,dd)
                try:
                    return datetime.datetime(y, m, dd)
                except Exception as e:
                    print(f"Exception for the data {d}---->", e)
                    return None
                
        try:
            rectify_date_vector_fun = np.vectorize(rectify_date)
            self.df.join_date = rectify_date_vector_fun(self.df.join_date)
            self.df = self.df.dropna(subset=["join_date"])

            # Capturing the exception cases with reason
            mask = (~self.copy_df["id"].isin(self.df["id"])) & (self.copy_df['reason'] == "")
            self.copy_df.loc[mask, "reason"] += self.copy_df.loc[mask, "reason"].astype(str) + "Invalid Date format"

            return json.loads(self.df.to_json(orient="records"))
        except Exception as e:
            print("Exception ---->", e)
            return None

    def transform_data(self):
        self.remove_empty_name()
        self.remove_invalid_email()
        self.remove_nega_sal()
        self.remove_invalid_date()        

    def load_data(self) -> bool:
        """Inserts data while also comparing for duplicacy

        Returns:
            bool: true if success, else false
        """
        try:
            # To make sure we are not pushing with same email again
            db = DatabaseHandler()
            out = db.df_to_sql(self.df, "employees_processed", "tmp")
            if out:
                print("# Data pushed Transformed data")
            else:
                raise Exception("Data was not pushed successfully.")
        except Exception as e:
            print("Exception ---->", e)
            return False

    def load_outlier_data(self) -> bool:
        """Inserts data while also comparing for duplicacy

        Returns:
            bool: true if success, else false
        """
        try:
            # To make sure we are not pushing with same email again
            # data = self.copy_df[~self.copy_df["id"].isin(self.df["id"])]
            db = DatabaseHandler()
            out = db.df_to_sql(self.copy_df[self.copy_df["reason"]!=""], "employees_unprocessed", "tmp")
            if out:
                print("# Data pushed Outlier data")
            else:
                raise Exception("Data was not pushed successfully.")
        except Exception as e:
            print("Exception ---->", e)
            return False