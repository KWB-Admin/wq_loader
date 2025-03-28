# Description

This is an ETL pipeline for taking `water_quality` data downloaded manually or via puppeteer from the BSK client site and loading it into the KWB data warehouse.

Once data is downloaded from BSK in `.csv` format, a powershell script moves the data and runs the python script.

# Requirements

Currently this relies on the following:

1. `psychopg2`,
2. `polars`,
3. `numpy`,
4. `PyYAML`.

Please see the requirements.txt file for specific versions and additional packages.

# Operations

To begin, if data contains anything from Ketzer Ranch, this is filtered out. Data from BSK contains test blank data, which is also filtered out. BSK data also is inconsistent in using the state well number data, so this data is payed special attention to make sure it comes in and matches what we use on the ops side. Timestamps are also stripped down to dates, since the timestamps are inconsistent in formating.

The data is then written as a `.parquet` file and loaded into the database.

# Historical data

There is a historical script which loads historical data. This data was processed from pdfs using OCR. Currently this is kept more for posterity, and shouldn't be used in production.