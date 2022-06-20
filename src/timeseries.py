"""
Create timeseries from latest data

Based on code by @tannervarrelman

"""
import io
import pandas as pd


def to_json(df: pd.DataFrame) -> str:
    return df.to_json(orient="records", date_format="iso", indent=2)


def to_csv(df: pd.DataFrame) -> str:
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue()


def by_confirmed(df: pd.DataFrame) -> pd.DataFrame:
    """Returns timeseries of counts and cumulative counts of cases"""

    confirmed = df[df.Status == "confirmed"]
    confirmed = confirmed.assign(Date=pd.to_datetime(confirmed.Date_confirmation))
    counts = confirmed.groupby("Date").size().resample("D").sum()
    return (
        pd.DataFrame([counts, counts.cumsum()])
        .T.reset_index()
        .rename(columns={0: "Cases", 1: "Cumulative_cases"})
    )


def by_country_confirmed(df: pd.DataFrame) -> pd.DataFrame:
    """Returns timeseries of counts and cumulative counts of cases by country"""
    confirmed = df[df.Status == "confirmed"]
    confirmed = confirmed.assign(Date=pd.to_datetime(confirmed.Date_confirmation))
    dfs = []
    for country, country_df in confirmed.groupby("Country"):
        country_counts = country_df.groupby("Date").size().resample("D").sum()
        dfs.append(
            pd.DataFrame([country_counts, country_counts.cumsum()])
            .T.reset_index()
            .rename(columns={0: "Cases", 1: "Cumulative_cases"})
            .assign(Country=country)
        )
    return pd.concat(dfs).reset_index().drop("index", axis=1)
