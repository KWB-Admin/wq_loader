# Description

This is an ETL pipeline for taking `water_quality` data downloaded manually from the BSK client site and loading it into the KWB data warehouse.

Once data is downloaded from BSK in `.csv` format, the files are dumped in the `data_dump` folder. From here, the ETL is ran following morning via Task Scheduler and the Power Shell script.

# Requirements

Currently this relies on two packages: `kwb_loader`, which will install three dependies:

1. `psychopg2`,
2. `polars`,
3. `numpy`;

and PyYAML.

Please see the requirements.txt file for specific versions and additional packages.

# Operations

To begin, if data contains anything from Ketzer Ranch, this is filtered. Data from BSK contains test blank data, which is filtered out. BSK data also is inconsistent in using the state well number data, so this data is payed special attention to make sure it comes in and matches what we use on the ops side. Timestamps are also stripped down to dates, since the timestamps are inconsistent in formating.

The data is then written as a `.parquet` file and loaded into the database.