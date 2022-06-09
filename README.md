# Monkeypox data

[![tests](https://github.com/globaldothealth/monkeypox/actions/workflows/tests.yml/badge.svg)](https://github.com/globaldothealth/monkeypox/actions/workflows/tests.yml) [![quality-checks](https://github.com/globaldothealth/monkeypox/actions/workflows/qc.yml/badge.svg)](https://github.com/globaldothealth/monkeypox/actions/workflows/qc.yml)

[![Monkeypox data GSheets -> S3 script deploy](https://github.com/globaldothealth/monkeypox/actions/workflows/data-script-deploy.yaml/badge.svg)](https://github.com/globaldothealth/monkeypox/actions/workflows/data-script-deploy.yaml) [![Monkeypox data S3 -> Github](https://github.com/globaldothealth/monkeypox/actions/workflows/data-transfer_S3-%3EGithub.yaml/badge.svg)](https://github.com/globaldothealth/monkeypox/actions/workflows/data-transfer_S3-%3EGithub.yaml)
## Contents

This repository contains dated records of curated Monkeypox cases from the 2022 outbreak (April - ), a data dictionary, and a script used to pull contents from a spreadsheet into JSON and CSV files.

The script is intended for use by the curation team and supporting engineers. It requires access to the relevant Google Sheet, and a Google Cloud service account.

The data dictionary is located in the root directory of this project. It contains information about columns/fields in the data sets.

The archives folder contains dated JSON and CSV files. They are currently uploaded manually; regularly and automatically updated data sets live in an (currently private) S3 bucket.

## Getting the data

**Direct download (CSV)**: https://raw.githubusercontent.com/globaldothealth/monkeypox/main/latest.csv  
**Direct download (JSON)**: https://raw.githubusercontent.com/globaldothealth/monkeypox/main/latest.json

**Python** (requires `pandas`):
```python
import pandas as pd
df = pd.read_csv("https://raw.githubusercontent.com/globaldothealth/monkeypox/main/latest.csv")
```

**R** (requires `httr`):
```r
library(httr)
df <- read.csv(text=content(GET("https://raw.githubusercontent.com/globaldothealth/monkeypox/main/latest.csv")))
```

## Contributing

If you would like to request changes, open an issue on this repository and we will happily consider your request. 
If requesting a fix please include steps to reproduce undesirable behaviors.

If you would like to contribute, assign an issue to yourself and/or reach out to a contributor and we will happily help you help us.

If you want to send data to us, you can use our template at [monkeypox-template.csv](monkeypox-template.csv) which makes
it easier for us to add to our list. Alternatively, you can open an issue with an attached CSV / XLSX file in this repository,
or email info@global.health

## Visualizations

* [Global.health](https://map.monkeypox.global.health/country): Map visualization of cases by country
* [Our World in Data](https://ourworldindata.org/monkeypox): cumulative and daily confirmed and suspected case counts and map
* [Healthmap](https://monkeypox.healthmap.org): Map visualization of cases by country

## License and attribution

This repository and data exports are published under the CC BY 4.0 license.

Please cite as: "Global.health Monkeypox (accessed on YYYY-MM-DD)" 

&

Kraemer, Tegally, Pigott, Dasgupta, Sheldon, Wilkinson, Schultheiss, et al. Tracking the 2022 Monkeypox Outbreak with Epidemiological Data in Real-Time. The Lancet Infectious Diseases. https://doi.org/10.1016/S1473-3099(22)00359-0.
