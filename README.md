# Monkeypox data

[![tests](https://github.com/globaldothealth/monkeypox/actions/workflows/src_tests.yml/badge.svg)](https://github.com/globaldothealth/monkeypox/actions/workflows/src_tests.yml) [![quality-checks](https://github.com/globaldothealth/monkeypox/actions/workflows/qc.yml/badge.svg)](https://github.com/globaldothealth/monkeypox/actions/workflows/qc.yml)

[![Monkeypox data GSheets -> S3 script deploy](https://github.com/globaldothealth/monkeypox/actions/workflows/data-script-deploy.yml/badge.svg)](https://github.com/globaldothealth/monkeypox/actions/workflows/data-script-deploy.yml) [![Monkeypox data S3 -> Github](https://github.com/globaldothealth/monkeypox/actions/workflows/data-transfer_S3-%3EGithub.yml/badge.svg)](https://github.com/globaldothealth/monkeypox/actions/workflows/data-transfer_S3-%3EGithub.yml)

## Contents

*Data are updated Monday - Friday.* 

#### Data changes
* **2022-07-07**: Only confirmed cases for Brazil are reported
* **2022-07-11**: From this date data files (`latest.csv`, `timeseries-*.csv`) have cases from the current outbreak, and from countries where MPXV is endemic. The lists are distinguished by the first letter of the ID, which is a string: **N** denoting cases from the current outbreak (equivalent to the current list), and **E** for cases from endemic countries.
* **2022-07-22**: Endemic data has been updated to accurately reflect confirmed/suspected/total cases from source reporting.

This repository contains dated records of curated Monkeypox cases from the 2022 outbreak (April - ), a [data dictionary](data_dictionary.yml), and a script used to pull contents from a spreadsheet into JSON and CSV files.

The script is intended for use by the curation team and supporting engineers. It requires access to the relevant Google Sheet, and a Google Cloud service account.

The [data dictionary](data_dictionary.yml) contains information about columns/fields in the data sets.

The analytics folder contains scripts that use the curated data set. This currently includes an R file that finds the risk of re-identification based on curated data.

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

Users can access archived line list, case definition, and ECDC files [here](https://7rydd2v2ra.execute-api.eu-central-1.amazonaws.com/web/). 
The archives folder contains timestamped JSON and CSV files.

## Contributing

If you would like to request changes, [open an issue](https://github.com/globaldothealth/monkeypox/issues/new) on this repository and we will happily consider your request. 
If requesting a fix please include steps to reproduce undesirable behaviors.

If you would like to contribute, assign an issue to yourself and/or reach out to a contributor and we will happily help you help us.

If you want to send data to us, you can use our template at [monkeypox-template.csv](monkeypox-template.csv) which makes
it easier for us to add to our list. Just open an issue and attach a CSV / XLSX file in this repository,
or email data to info@global.health. Remove any Personally Identifiable Information.

## Visualizations

* [Global.health](https://map.monkeypox.global.health/country): Map visualization of cases by country
* [Our World in Data](https://ourworldindata.org/monkeypox): cumulative and daily confirmed and suspected case counts and map

## License and attribution

This repository and data exports are published under the CC BY 4.0 license.

Please cite as: "Global.health Monkeypox (accessed on YYYY-MM-DD)" 

&

Kraemer, Tegally, Pigott, Dasgupta, Sheldon, Wilkinson, Schultheiss, et al. Tracking the 2022 Monkeypox Outbreak with Epidemiological Data in Real-Time. The Lancet Infectious Diseases. https://doi.org/10.1016/S1473-3099(22)00359-0.

For ECDC files, please cite (files are licensed under [Creative Commons Attribution 4.0 International](https://creativecommons.org/licenses/by/4.0/legalcode)):

European Centre for Disease Prevention and Control/WHO Regional Office for Europe. Monkeypox, Joint Epidemiological overview, {day} {month}, 2022
