import polars as pl
from kwb_loader import loader
from yaml import load, Loader
import os

user = os.getenv("kwb_dw_user")
host = os.getenv("kwb_dw_host")
password = os.getenv("kwb_dw_password")
hist_pdf_table = 'eurofins_lab_results_from_pdfs'

if __name__ == "__main__":
    etl_yaml = load(open("yaml/etl_variables.yaml", "r"), Loader)

    file_path = etl_yaml["historical_data_file_path"]

    loader.load(
        credentials=(user, host, password),
        dbname=etl_yaml["db_name"],
        schema=etl_yaml["schema"],
        table_name=hist_pdf_table,
        data_path=file_path,
    )