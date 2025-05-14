import psycopg2 as pg
from psycopg2 import sql
import polars as pl
from numpy import ndarray
from datetime import datetime, date
from yaml import load, Loader
import logging, os

logging.basicConfig(
    filename="log/wq_etl.log",
    encoding="utf-8",
    filemode="a",
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
)

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

user = os.getenv("kwb_dw_user")
host = os.getenv("kwb_dw_host")
password = os.getenv("kwb_dw_password")


def transform_wq_data(data: pl.DataFrame, etl_yaml: dict) -> pl.DataFrame:
    """
    This takes water quality data downloaded from the BSK client portal in .csv
    format and transforms it into a file ready for loading

    Args:
        data: pl.DataFrame, uncleaned/untrasformed data
        etl_yaml: dict, variables used for cleaning
    Returns:
        data: pl.Dataframe, cleaned data
    """
    try:
        data = data.select([col for col in etl_yaml["new_col_names"].keys()]).rename(
            etl_yaml["new_col_names"]
        )
        data = data.filter(
            (~pl.col("state_well_number").str.contains("Field Blank"))
            & (~pl.col("state_well_number").str.contains("TB"))
            & (~pl.col("state_well_number").str.contains("TCP"))
        ).drop_nulls("result")

        data = fix_well_name(data)
        for key, val in etl_yaml["schema"].items():
            if val == "timestamp":
                data = data.with_columns(
                    pl.col(key).str.to_datetime("%m/%d/%Y %I:%M:%S %p")
                )
            elif val == "real":
                data = data.with_columns(pl.col(key).cast(pl.Float64))
        logger.info("Data successfully cleaned and transformed.")
        return data
    except:
        logging.error("")


def fix_well_name(data: pl.DataFrame) -> pl.DataFrame:
    """
    Takes water quality data and transforms the well numbers to match
    up with their actual values

    Args:
        data: pl.DataFrame, water quality data with original state well number values

    Returns:
        data: pl.DataFrame, water quality data with fixed state well number values
    """
    data = data.with_columns(
        pl.when(pl.col("state_well_number").str.contains("-"))
        .then(pl.col("state_well_number").str.replace_all(" ", ""))
        .otherwise(pl.col("state_well_number").str.replace(" ", "-"))
    ).with_columns(pl.col("state_well_number").str.replace_all(" ", ""))

    data = data.with_columns(pl.col("state_well_number").str.len_chars().alias("len"))
    data = data.with_columns(
        pl.when(pl.col("len") == 12)
        .then(pl.col("state_well_number").str.replace("-", "-0"))
        .when(pl.col("len") == 11)
        .then(pl.col("state_well_number") + "01")
        .otherwise(pl.col("state_well_number"))
    )
    data = data.with_columns(
        pl.when(pl.col("state_well_number") == "30S/25E-ISH01")
        .then(pl.col("state_well_number").str.replace("IS", "15"))
        .when(pl.col("state_well_number") == "30S/25E-020D1")
        .then(pl.col("state_well_number").str.replace("020D1", "20D01"))
        .when(pl.col("state_well_number").str.contains("30E/25S"))
        .then(pl.col("state_well_number").str.replace("30E/25S", "30S/25E"))
        .otherwise(pl.col("state_well_number")),
    )

    return data.drop("len")


def get_pg_connection(db_name: str) -> pg.extensions.connection:
    """
    This tests a connection with a postgres database to ensure that
    we're loading into a database that actually exists.

    Args:
        db_name: str, name of database to connect to.
    Returns:
        con: pg.extensions.connection, psycopg connection to pg database
    """
    try:
        con = pg.connect(
            "dbname=%s user=%s host=%s password=%s" % (db_name, user, host, password)
        )
        con.autocommit = True
        logging.info("Successfully connected to %s db" % (db_name))
        return con

    except pg.OperationalError as Error:
        logging.error(Error)


def check_table_exists(con: pg.extensions.connection, etl_yaml: dict):
    """
    This tests a to ensure the table we'll be writing to exists in
    the postgres schema provided.

    Args:
        con: pg.extensions.connection, psycopg connection to pg
            database
        etl_yaml: dict, variables used for cleaning
    """
    cur = con.cursor()
    command = sql.SQL(
        """
        Select * from {schema_name}.{table_name} limit 1  
        """
    ).format(
        schema_name=sql.Identifier(etl_yaml["schema_name"]),
        table_name=sql.Identifier(etl_yaml["table_name"]),
    )
    try:
        cur.execute(command)
        if isinstance(cur.fetchall(), list):
            logging.info("Table exists, continue with loading.")
    except pg.OperationalError as Error:
        logging.error(Error)


def load_data_into_pg_warehouse(data: pl.DataFrame, etl_yaml: dict):
    """
    This loads data into the KWB data warehouse, hosted in a postgres db.

    Args:
        data: polars.DataFrame, data to be loaded into warehouse
        etl_yaml: dict, general variables for the etl process
    """
    con = get_pg_connection(etl_yaml["db_name"])
    check_table_exists(con, etl_yaml)
    try:
        cur = con.cursor()
        for row in data.to_numpy():
            query = build_load_query(row, etl_yaml)
            cur.execute(query)
        cur.close()
        con.close()
        logging.info(
            "Data was successfully loaded to %s.%s.%s"
            % (etl_yaml["db_name"], etl_yaml["schema_name"], etl_yaml["table_name"])
        )
    except pg.OperationalError as Error:
        con.close()
        logging.error(Error)


def build_load_query(data: ndarray, etl_yaml: dict) -> pg.sql.Composed:
    """
    This loads data into the KWB data warehouse, hosted in a postgres db.

    Args:
        data: numpy.ndarray, row of data to be loaded
        etl_yaml: dict, general variables for the etl process
    Returns:
        pg.sql.Composed, Upsert query used to load data
    """
    col_names = sql.SQL(", ").join(
        sql.Identifier(col) for col in etl_yaml["schema"].keys()
    )
    values = sql.SQL(" , ").join(sql.Literal(val) for val in data)
    update_cols = sql.SQL(" , ").join(
        sql.SQL(f"{col} = Excluded.{col}") for col in etl_yaml["update_cols"]
    )
    return sql.SQL(
        """
        INSERT INTO {schema_name}.{table_name} ({col_names}) VALUES ({values})
        ON CONFLICT ({prim_key}) DO UPDATE SET {update_cols}
        """
    ).format(
        schema_name=sql.Identifier(etl_yaml["schema_name"]),
        table_name=sql.Identifier(etl_yaml["table_name"]),
        col_names=col_names,
        values=values,
        prim_key=sql.SQL(etl_yaml["primary_key"]),
        update_cols=update_cols,
    )


if __name__ == "__main__":
    logger.info(
        "--------------- Water Quality ETL running on %s ----------------"
        % (datetime.today())
    )

    if not os.listdir("data_dump"):
        logger.info("No data is available for loading, quitting program.\n")
        exit()

    etl_yaml = load(open("yaml/etl_variables.yaml", "r"), Loader)
    logger.info("Loaded etl yaml.")

    for raw_data_file in os.listdir("data_dump"):

        data = pl.read_csv(
            source=f"data_dump/{raw_data_file}", infer_schema_length=10000
        )

        data = data.filter(~(pl.col("Sample.SampleName").str.contains("Kretzer")))
        if data.is_empty():
            logger.info("Data is for Ketzer Ranch, not to be uploaded to DW")
            os.remove("data_dump/%s" % (raw_data_file))
            continue

        data = transform_wq_data(data=data, etl_yaml=etl_yaml)

        load_data_into_pg_warehouse(data=data, etl_yaml=etl_yaml)

        new_file_path = f"loaded_data/bsk_data_loaded_{date.today()}.parquet"
        data.write_parquet(new_file_path)
        os.remove("data_dump/%s" % (raw_data_file))

    logging.info("Successfully ran Water Quality ETL.\n")
