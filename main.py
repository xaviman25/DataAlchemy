from etl_processor import ETLProcessor
import time

if __name__ == "__main__":
    """Running the ETL
    """
    start = time.time()
    e = ETLProcessor()
    e.read_data()
    print("\n# Read data")
    print(e.df, "\n")
    job1 = time.time()
    print("\n# Time to complete Extraction ---->", job1-start, "\n")
    e.transform_data()
    print("\n# Transformed data")
    print(e.df)
    job2 = time.time()
    print("\n# Time to complete Transform ---->", job2-job1, "\n")
    e.load_data()
    e.load_outlier_data()
    job3 = time.time()
    print("\n# Time to complete Load ---->", job3-job2, "\n")
    print("\nTime to finish up ETL job ---->", time.time()-start, "\n")
