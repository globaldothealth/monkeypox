# Monkeypox data

[![tests](https://github.com/globaldothealth/monkeypox/actions/workflows/tests.yml/badge.svg)](https://github.com/globaldothealth/monkeypox/actions/workflows/tests.yml) [![quality-checks](https://github.com/globaldothealth/monkeypox/actions/workflows/qc.yml/badge.svg)](https://github.com/globaldothealth/monkeypox/actions/workflows/qc.yml)

[![Monkeypox data GSheets -> S3 script deploy](https://github.com/globaldothealth/monkeypox/actions/workflows/data-script-deploy.yaml/badge.svg)](https://github.com/globaldothealth/monkeypox/actions/workflows/data-script-deploy.yaml) [![Monkeypox data S3 -> Github](https://github.com/globaldothealth/monkeypox/actions/workflows/data-transfer_S3-%3EGithub.yaml/badge.svg)](https://github.com/globaldothealth/monkeypox/actions/workflows/data-transfer_S3-%3EGithub.yaml)
## Contents

*Data are updated Monday - Friday.*

#### Upcoming change
* From **2022-07-07 onwards, only confirmed cases for Brazil will be reported**
* :warning: From **2022-07-11, all data files** (`latest.csv`, `timeseries-*.csv`) **will have cases from the current outbreak, and from countries where MPXV is endemic**. The lists will be distinguished by the first letter of the ID, which will be of string type: **N** denoting cases from the current outbreak (equivalent to the current list), and **E** for cases from endemic countries.

This repository contains dated records of curated Monkeypox cases from the 2022 outbreak (April - ), a data dictionary, and a script used to pull contents from a spreadsheet into JSON and CSV files.

The script is intended for use by the curation team and supporting engineers. It requires access to the relevant Google Sheet, and a Google Cloud service account.

The data dictionary is located in the root directory of this project. It contains information about columns/fields in the data sets.

The archives folder contains dated JSON and CSV files. They are currently uploaded manually; regularly and automatically updated data sets live in an (currently private) S3 bucket.

There is also a daily briefing report generated from this data at https://www.monkeypox.global.health

## Getting the data

**Line list (CSV)**: https://raw.githubusercontent.com/globaldothealth/monkeypox/main/latest.csv  
**Line list (JSON)**: https://raw.githubusercontent.com/globaldothealth/monkeypox/main/latest.json

**Timeseries**: https://raw.githubusercontent.com/globaldothealth/monkeypox/main/timeseries-confirmed.csv  
**Timeseries by country**: https://raw.githubusercontent.com/globaldothealth/monkeypox/main/timeseries-country-confirmed.csv

**Python** (requires `pandas`):
```python
import pandas as pd
df = pd.read_csv("https://raw.githubusercontent.com/globaldothealth/monkeypox/main/latest.csv")
```

**R** :
```r
df <- read.csv("https://raw.githubusercontent.com/globaldothealth/monkeypox/main/latest.csv")
```

## Contributing

If you would like to request changes, open an issue on this repository and we will happily consider your request. 
If requesting a fix please include steps to reproduce undesirable behaviors.

If you would like to contribute, assign an issue to yourself and/or reach out to a contributor and we will happily help you help us.

If you want to send data to us, you can use our template at [monkeypox-template.csv](monkeypox-template.csv) which makes
it easier for us to add to our list. Just open an issue and attach a CSV / XLSX file in this repository,
or email data to info@global.health. Remove any Personally Identifiable Information.

## Visualizations

* [Global.health](https://map.monkeypox.global.health/country): Map visualization of cases by country
* [Our World in Data](https://ourworldindata.org/monkeypox): cumulative and daily confirmed and suspected case counts and map

## License and attribution

This repository and data exports (except files in the *ecdc* folder) are published under the CC BY 4.0 license.

Please cite as: "Global.health Monkeypox (accessed on YYYY-MM-DD)" 

&

Kraemer, Tegally, Pigott, Dasgupta, Sheldon, Wilkinson, Schultheiss, et al. Tracking the 2022 Monkeypox Outbreak with Epidemiological Data in Real-Time. The Lancet Infectious Diseases. https://doi.org/10.1016/S1473-3099(22)00359-0.

For files in the *ecdc* folder, please cite (reproduction is authorized, provided the source is acknowledged):

European Centre for Disease Prevention and Control/WHO Regional Office for Europe. Monkeypox, Joint Epidemiological overview, {day} {month}, 2022
