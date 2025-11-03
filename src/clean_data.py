import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay

US_BD = CustomBusinessDay(calendar=USFederalHolidayCalendar())  # FRED/US markets

def forward_fill_missing_data(df: pd.DataFrame):
    return df.ffill()

def percent_to_decimal(df: pd.DataFrame, columns: list[str]):
    for col in columns:
        df[col] = df[col] / 100.0
        df[col] = df[col].round(7)
    return df

def check_anomalies_macroeconomic(df: pd.DataFrame):
    # Ensure no negative values in 'value' column
    if (df['value'] < 0).any():
        raise ValueError("Anomaly detected: Negative values found in 'value' column.")
    
    # Ensure no duplicate rows
    if (df['date'].duplicated()).any():
        raise ValueError("Anomaly detected: Duplicate rows found.")
    
    # Ensure that there are no more null values
    if df.isnull().values.any():
        raise ValueError("Anomaly detected: Null values found after cleaning.")
    
    return df

def check_time_gaps(df: pd.DataFrame):
    d = df.copy()
    d['date'] = pd.to_datetime(d['date']).dt.normalize()
    d = d.sort_values('date')
    expected = pd.date_range(d['date'].min(), d['date'].max(), freq=US_BD)
    if not expected.equals(pd.DatetimeIndex(d['date'])):
        missing = expected.difference(pd.DatetimeIndex(d['date']))
        if len(missing):
            raise ValueError(f"Anomaly: missing {len(missing)} business dates, e.g. {missing[:5].tolist()}")
    
    return df

def create_rows_for_missing_dates(df: pd.DataFrame):
    # Convert date to datetime, normalize to midnight UTC, then remove timezone
    df['date'] = pd.to_datetime(df['date'], utc=True).dt.normalize().dt.tz_localize(None)
    df = df.set_index('date').sort_index()
    
    # Create business day index and reindex
    bday_index = pd.date_range(start=df.index.min(), end=df.index.max(), freq=US_BD)
    df = df.reindex(bday_index)
    #df = forward_fill_missing_data(df)
    
    return df

def group_by_date(df: pd.DataFrame):
    df['date'] = pd.to_datetime(df['date'])
    df['date'] = df['date'].dt.date
    df = df.groupby('date').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).reset_index()
    df['date'] = pd.to_datetime(df['date'])
    # Set date as index for joining
    df = df.set_index('date')
    return df

if __name__ == "__main__":

    # AAA
    AAA = pd.read_csv(f"data/raw/AAA.csv")
    AAA = forward_fill_missing_data(AAA)
    AAA = percent_to_decimal(AAA, ['value'])
    AAA = check_anomalies_macroeconomic(AAA)
    AAA['date'] = pd.to_datetime(AAA['date'])
    AAA = AAA.set_index('date')
    
    # BAA
    BAA = pd.read_csv(f"data/raw/BAA.csv")
    BAA = forward_fill_missing_data(BAA)
    BAA = percent_to_decimal(BAA, ['value'])
    BAA = check_anomalies_macroeconomic(BAA)
    BAA['date'] = pd.to_datetime(BAA['date'])
    BAA = BAA.set_index('date')
    
    # credit_spread_baa_aaa
    spread_baa_aaa = pd.read_csv(f"data/raw/credit_spread_baa_aaa.csv")
    spread_baa_aaa = forward_fill_missing_data(spread_baa_aaa)
    spread_baa_aaa = percent_to_decimal(spread_baa_aaa, ['value'])
    spread_baa_aaa = check_anomalies_macroeconomic(spread_baa_aaa)
    spread_baa_aaa['date'] = pd.to_datetime(spread_baa_aaa['date'])
    spread_baa_aaa = spread_baa_aaa.set_index('date')
    spread_baa_aaa = spread_baa_aaa.rename(columns={'value': 'credit_spread_baa_aaa'})

    # DGS2
    DGS2 = pd.read_csv(f"data/raw/DGS2.csv")
    DGS2 = forward_fill_missing_data(DGS2)
    DGS2 = percent_to_decimal(DGS2, ['value'])
    DGS2 = check_anomalies_macroeconomic(DGS2)
    DGS2 = check_time_gaps(DGS2)
    DGS2['date'] = pd.to_datetime(DGS2['date'])
    DGS2 = DGS2.set_index('date')
    
    # DGS10
    DGS10 = pd.read_csv(f"data/raw/DGS10.csv")
    DGS10 = forward_fill_missing_data(DGS10)
    DGS10 = percent_to_decimal(DGS10, ['value'])
    DGS10 = check_anomalies_macroeconomic(DGS10)
    DGS10 = check_time_gaps(DGS10)
    DGS10['date'] = pd.to_datetime(DGS10['date'])
    DGS10 = DGS10.set_index('date')
    
    # yield_curve_10y_2y_spread
    curve_10y_2y = pd.read_csv(f"data/raw/yield_curve_10y_2y_spread.csv")
    curve_10y_2y = forward_fill_missing_data(curve_10y_2y)
    curve_10y_2y = percent_to_decimal(curve_10y_2y, ['value'])
    curve_10y_2y = check_anomalies_macroeconomic(curve_10y_2y)
    curve_10y_2y = check_time_gaps(curve_10y_2y)
    curve_10y_2y['date'] = pd.to_datetime(curve_10y_2y['date'])
    curve_10y_2y = curve_10y_2y.set_index('date')
    curve_10y_2y = curve_10y_2y.rename(columns={'value': 'yield_curve_10y2y'})
    
    # SPY
    SPY = pd.read_csv(f"data/raw/SPY_1min_20231027_20251027.csv")
    SPY = SPY.rename(columns={"caldt": "date"})
    SPY = SPY[['date', 'open', 'high', 'low', 'close', 'volume']]
    SPY = group_by_date(SPY)
    SPY = SPY.reset_index()
    SPY = create_rows_for_missing_dates(SPY)

    # VIX
    VIX = pd.read_csv("data/raw/^VIX_1day_20231027_20251027.csv")
    VIX = VIX.rename(columns={"caldt": "date"})
    VIX = VIX[['date', 'open', 'high', 'low', 'close', 'volume']]
    VIX = create_rows_for_missing_dates(VIX)

    # Merge of all cleaned data
    df_final = SPY.join(VIX, lsuffix='_SPY', rsuffix='_VIX', how='outer')
    df_final = df_final.join(curve_10y_2y, how='outer')
    df_final = df_final.join(spread_baa_aaa, how='outer')
    # Forward fill for monthly values
    df_final = df_final.ffill()
    df_final = df_final.dropna() # Remove any remaining NaN values, so that remains the last year
    df_final.to_csv("data/processed/market_macro_merged.csv", index=True, index_label='date')
    df_final.to_parquet("data/processed/market_macro_merged.parquet", index=True)
