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

polars_schema = {
    "Sample.Wrk": pl.String,
    "Sample.Sample": pl.String,
    "Sample.SampleName": pl.String,
    "Sample.LogMatrix": pl.String,
    "Sample.Sampled": pl.String,
    "Analyte.Analysis": pl.String,
    "Analyte.Analyte": pl.String,
    "Analyte.tMDL": pl.Float64,
    "Analyte.tMRL": pl.Float64,
    "Analyte.tResult": pl.String,
    "Analyte.RptUnits": pl.String,
}


def transform_wq_data(
    cols_to_drop: list,
    new_cols_in_order: list,
    new_cols_dict: dict,
    data: pl.DataFrame,
) -> pl.DataFrame:
    """
    This takes water quality data downloaded from the BSK client portal in .csv
    format and transforms into a .parquet file ready for loading

    Args:
        cols_to_drop: list, contains columns that aren't needed
        new_cols_in_order: list, contains columns in correct order for
            eventual loading
        new_cols_dict: dict, contains dictionary where keys are the old column
        names and values are the new column names

    Returns:
        data: pl.Dataframe, cleaned data
    """
    try:
        data = data.rename(new_cols_dict).with_columns(
            pl.lit((datetime.today())).alias("date_added")
        )
        data = data.filter(
            (~pl.col("state_well_number").str.contains("Field Blank"))
            & (~pl.col("state_well_number").str.contains("TB"))
            & (~pl.col("state_well_number").str.contains("TCP"))
        ).drop_nulls("result")

        data = fix_well_name(data)
        logger.info("Data successfully cleaned and transformed")
        return data.drop(cols_to_drop)[new_cols_in_order].with_columns(
            pl.col("sample_date").str.replace(" (.*)", "")
        )
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

    return data


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


def check_table_exists(
    con: pg.extensions.connection, schema_name: str, table_name: str
):
    """
    This tests a to ensure the table we'll be writing to exists in
    the postgres schema provided.

    Args:
        con: pg.extensions.connection, psycopg connection to pg
            database
        schema_name: str, name of postgres schema
        table_name: str, name of table
    """
    cur = con.cursor()
    command = sql.SQL(
        """
        Select * from {schema_name}.{table_name} limit 1  
        """
    ).format(
        schema_name=sql.Identifier(schema_name),
        table_name=sql.Identifier(table_name),
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
    check_table_exists(con, etl_yaml["db_name"], etl_yaml["table_name"])
    try:
        cur = con.cursor()
        for row in data.to_numpy():
            query = build_load_query(row, etl_yaml)
            cur.execute(query)
        cur.close()
        con.close()
        logging.info(
            "Data was successfully loaded to %s.%s"
            % (etl_yaml["db_name"], etl_yaml["table_name"])
        )
    except:
        con.close()
        logging.error("Error occurred during loading.")
    return


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
        sql.Identifier(col) for col in etl_yaml["new_columns_in_order"]
    )
    values = sql.SQL(" , ").join(sql.Literal(val) for val in data)
    return sql.SQL(
        """
        INSERT INTO {schema_name}.{table} ({col_names}) VALUES ({values})
        ON CONFLICT {prim_key} DO UPDATE SET {update_col} = Excluded.{update_col}
        """
    ).format(
        schema_name=sql.Identifier(etl_yaml["schema_name"]),
        table=sql.Identifier(etl_yaml["table"]),
        col_names=col_names,
        values=values,
        prim_key=sql.SQL(etl_yaml["prim_key"]),
        update_col=sql.Identifier(etl_yaml["update_col"]),
    )


if __name__ == "__main__":
    logger.info(
        "--------------- Water Quality ETL ran on %s ----------------"
        % (datetime.today())
    )

    if not os.listdir("data_dump"):
        logger.info("No data is available for loading, quitting program.")
        exit()

    etl_yaml = load(open("yaml/etl_variables.yaml", "r"), Loader)
    logger.info("Loaded etl_variables.yaml")

    new_cols_dict = {
        key: etl_yaml["new_column_names"][ind]
        for ind, key in enumerate(polars_schema.keys())
    }

    for raw_data_file in os.listdir("data_dump"):

        new_file_path = f"data_dump/bsk_cleaned_data_{date.today()}.parquet"

        data = pl.read_csv(source=f"data_dump/{raw_data_file}", schema=polars_schema)

        data = data.filter(pl.col("Sample.SampleName").str.contains("Ketzer"))
        if data.is_empty():
            logger.info("Data is for Ketzer Ranch, not to be uploaded to DW")
            os.remove("data_dump/%s" % (raw_data_file))
            continue

        data = transform_wq_data(
            cols_to_drop=etl_yaml["columns_to_drop"],
            new_cols_in_order=etl_yaml["new_columns_in_order"],
            new_cols_dict=new_cols_dict,
            data=data,
        )

        data.write_parquet(new_file_path)

        load_data_into_pg_warehouse(data=data, etl_yaml=etl_yaml)

        os.rename(
            new_file_path, "loaded_data/bsk_data_loaded_%s.parquet" % (date.today())
        )
        os.remove("data_dump/%s" % (raw_data_file))

    logging.info("Successfully Water Quality ETL.\n")
