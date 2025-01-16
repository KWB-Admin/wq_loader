from kwb_loader import loader
import polars as pl
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

schema = {
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
    format and transforms into a .parquet file ready to loading

    Args:
        cols_to_drop: list, contains columns that aren't needed
        new_cols_in_order: list, contains columns in correct order for
            eventual loading
        new_cols_dict: dict, contains dictionary where keys are the old column
        names and values are the new column names

    Returns:
        data: pl.Dataframe, cleaned data
    """
    data = data.rename(new_cols_dict).with_columns(
        pl.lit((datetime.today())).alias("date_added")
    )
    data = data.filter(
        (~pl.col("state_well_number").str.contains("Field Blank"))
        & (~pl.col("state_well_number").str.contains("TB"))
        & (~pl.col("state_well_number").str.contains("TCP"))
    ).drop_nulls("result")

    data = fix_well_name(data)

    return data.drop(cols_to_drop)[new_cols_in_order].with_columns(
        pl.col("sample_date").str.replace(" (.*)", "")
    )


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
        key: etl_yaml["new_column_names"][ind] for ind, key in enumerate(schema.keys())
    }

    for raw_data_file in os.listdir("data_dump"):

        new_file_path = f"data_dump/bsk_cleaned_data_{date.today()}.parquet"

        data = pl.read_csv(source=f"data_dump/{raw_data_file}", schema=schema)
        try:
            data = transform_wq_data(
                cols_to_drop=etl_yaml["columns_to_drop"],
                new_cols_in_order=etl_yaml["new_columns_in_order"],
                new_cols_dict=new_cols_dict,
                data=data,
            )
            logger.info("Data successfully cleaned and transformed")
        except:
            logger.exception("..")

        data.write_parquet(new_file_path)

        loader.load(
            credentials=(user, host, password),
            dbname=etl_yaml["db_name"],
            schema=etl_yaml["schema"],
            table_name=etl_yaml["table"],
            data_path=new_file_path,
        )

        logging.info(
            "Successfully loaded data into %s.%s.%s \n"
            % (etl_yaml["db_name"], etl_yaml["schema"], etl_yaml["table"])
        )

        os.rename(
            new_file_path, "loaded_data/bsk_data_loaded_%s.parquet" % (date.today())
        )
        os.remove("data_dump/%s" % (raw_data_file))
