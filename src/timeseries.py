"""
Create timeseries from latest data

Based on code by @tannervarrelman

"""
import io
import pandas as pd

today = pd.Timestamp.today()

UK_COUNTRIES = ["England", "Wales", "Scotland", "Northern Ireland"]

def to_json(df: pd.DataFrame) -> str:
    return df.to_json(orient="records", date_format="iso", indent=2)


def to_csv(df: pd.DataFrame) -> str:
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue()


def by_confirmed(df: pd.DataFrame, last_date: pd.Timestamp = None) -> pd.DataFrame:
    """Returns timeseries of counts and cumulative counts of cases"""

    last_date = last_date or today
    confirmed = df[df.Status == "confirmed"]
    confirmed = confirmed.assign(Date=pd.to_datetime(confirmed.Date_confirmation))
    counts = confirmed.groupby("Date").size().resample("D").sum()
    counts = counts.reindex(pd.date_range(counts.index.min(), last_date), fill_value=0)
    return (
        pd.DataFrame([counts, counts.cumsum()])
        .T.reset_index()
        .rename(columns={0: "Cases", 1: "Cumulative_cases", "index": "Date"})
    )


def by_country_confirmed(
    df: pd.DataFrame, last_date: pd.Timestamp = None
) -> pd.DataFrame:
    """Returns timeseries of counts and cumulative counts of cases by country"""

    last_date = last_date or today
    confirmed = df[df.Status == "confirmed"]
    confirmed = confirmed.assign(
        Date=pd.to_datetime(confirmed.Date_confirmation),
        Country=confirmed.Country.replace(UK_COUNTRIES, "United Kingdom"),
    )
    dfs = []
    for country, country_df in confirmed.groupby("Country"):
        country_counts = country_df.groupby("Date").size().resample("D").sum()
        country_counts = country_counts.reindex(
            pd.date_range(country_counts.index.min(), last_date),
            fill_value=0,
        )
        dfs.append(
            pd.DataFrame([country_counts, country_counts.cumsum()])
            .T.reset_index()
            .rename(columns={0: "Cases", 1: "Cumulative_cases", "index": "Date"})
            .assign(Country=country)
        )
    return pd.concat(dfs).reset_index().drop("index", axis=1)
