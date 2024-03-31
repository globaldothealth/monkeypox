# Mpox data

## Contents

The Global.health team completed a 100 days mission to provide decision makers, researchers, and the public with timely and accurate, openly-accessible, global line-list data for the 2022 Mpox outbreak. Now, we are at a point of transition. On 2022-09-23, the Global.health team will shift from providing manually-curated line-list data to openly-available data resources, compiling downloadable Mpox datasets with aggregate case data from the World Health Organization, U.S. Centers for Disease Control and Prevention, and the European Centre for Disease Control and Prevention. Global.health Mpox line-list data, last updated 2022-09-22, will remain accessible via download through GitHub. We thank our user community for their many helpful contributions and for identifying Global.health as a trusted source of information.​

Users can access the latest data set, and archived line list, case definition, and ECDC files [here](https://7rydd2v2ra.execute-api.eu-central-1.amazonaws.com/web/). 
The archives folder contains timestamped JSON and CSV files.

#### Data changes
* **2022-09-23**: Deprecated `latest.csv` file.
* **2022-09-22**: End of line list data. 
* **2022-07-07**: Only confirmed cases for Brazil are reported
* **2022-07-11**: From this date data files (`latest.csv`, `timeseries-*.csv`) have cases from the current outbreak, and from countries where MPXV is endemic. The lists are distinguished by the first letter of the ID, which is a string: **N** denoting cases from the current outbreak (equivalent to the current list), and **E** for cases from endemic countries.
* **2022-07-22**: Endemic data has been updated to accurately reflect confirmed/suspected/total cases from source reporting.

This repository contains dated records of curated Mpox cases from the 2022 outbreak (April - ), a [data dictionary](data_dictionary.yml), and a script used to pull contents from a spreadsheet into JSON and CSV files.

The script is intended for use by the curation team and supporting engineers. It requires access to the relevant Google Sheet, and a Google Cloud service account.

The [data dictionary](data_dictionary.yml) contains information about columns/fields in the data sets. It is deprecated as of 2022-09-23.

The analytics folder contains scripts that use the curated data set. This currently includes an R file that finds the risk of re-identification based on curated data.

There is also a daily briefing report generated from this data at https://www.monkeypox.global.health

## Getting the data

### Line list with aggregated counts from WHO and CDC
[CSV](https://7rydd2v2ra.execute-api.eu-central-1.amazonaws.com/web/url?folder=&file_name=latest.csv)  

### Deprecated: Final line list from Global.health, as of 2022-09-22
[CSV](https://raw.githubusercontent.com/globaldothealth/monkeypox/946edb545947af7f5195459ce52bb71d098e240c/latest_deprecated.csv)
[Timeseries](https://raw.githubusercontent.com/globaldothealth/monkeypox/946edb545947af7f5195459ce52bb71d098e240c/timeseries-confirmed-deprecated.csv)
[Timeseries by country](https://raw.githubusercontent.com/globaldothealth/monkeypox/946edb545947af7f5195459ce52bb71d098e240c/timeseries-country-confirmed-deprecated.csv)

### Python
```python
import pandas as pd
df = pd.read_csv("https://7rydd2v2ra.execute-api.eu-central-1.amazonaws.com/web/url?folder=&file_name=latest.csv")
```
### R
```r
df <- read.csv("https://7rydd2v2ra.execute-api.eu-central-1.amazonaws.com/web/url?folder=&file_name=latest.csv")
```

## Contributing

If you would like to request changes, [open an issue](https://github.com/globaldothealth/monkeypox/issues/new) on this repository and we will happily consider your request. 
If requesting a fix please include steps to reproduce undesirable behaviors.

If you would like to contribute, assign an issue to yourself and/or reach out to a contributor and we will happily help you help us.

If you want to send data to us, just open an issue and attach a CSV / XLSX file in this repository,
or email data to info@global.health. Remove any Personally Identifiable Information.

## Visualizations

* [Global.health](https://map.monkeypox.global.health/country): Map visualization of cases by country
* [Our World in Data](https://ourworldindata.org/monkeypox): cumulative and daily confirmed and suspected case counts and map

## License and attribution

This repository and data exports are published under the CC BY 4.0 license.

Please cite as: "Global.health Mpox (accessed on YYYY-MM-DD)" 

&

Kraemer, Tegally, Pigott, Dasgupta, Sheldon, Wilkinson, Schultheiss, et al. Tracking the 2022 Mpox Outbreak with Epidemiological Data in Real-Time. The Lancet Infectious Diseases. https://doi.org/10.1016/S1473-3099(22)00359-0.

For ECDC files, please cite (files are licensed under [Creative Commons Attribution 4.0 International](https://creativecommons.org/licenses/by/4.0/legalcode)):

European Centre for Disease Prevention and Control/WHO Regional Office for Europe. Mpox, Joint Epidemiological overview, {day} {month}, 2022
