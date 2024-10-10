from kwb_loader import loader
import polars as pl
from datetime import datetime, date
from yaml import load, Loader

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
    new_file: str,
    new_cols_dict: dict,
    data: pl.DataFrame,
):
    """
    This takes water quality data downloaded from the BSK client portal in .csv
    format and transforms into a .parquet file ready to loading

    Args:
        cols_to_drop: list, contains columns that aren't needed
        new_cols_in_order: list, contains columns in correct order for
            eventual loading
        new_file: str, path of parquet file created at end of function
        new_cols_dict: dict, contains dictionary where keys are the old column
        names and values are the new column names
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

    data.drop(cols_to_drop)[new_cols_in_order].with_columns(
        pl.col("sample_date").str.replace(" (.*)", "")
    ).write_parquet(new_file)


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
    # yaml contains column variables for use in different functions
    wq_yml = load(open("columns_vars.yaml", "r"), Loader)

    new_file = f"data_dump/bsk_cleaned_data_{date.today()}.parquet"
    new_cols_dict = {
        key: wq_yml["new_cols"][ind] for ind, key in enumerate(schema.keys())
    }

    data = pl.read_csv("data_dump\Simple_Results__09242024_1448.csv", schema=schema)
    transform_wq_data(
        wq_yml["cols_to_drop"],
        wq_yml["new_cols_in_order"],
        new_file,
        new_cols_dict,
        data,
    )
    loader.load("kwb", "water_quality", "bsk_lab_results", new_file)
