import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay

US_BD = CustomBusinessDay(calendar=USFederalHolidayCalendar()) # FRED/US markets

DATASET_NAMES = {
    "SPY_1min_20231027_20251027": "SPY",
    "yield_curve_10y_2y_spread": "Curve 10y-2y",
    "^VIX_1day_20231027_20251027": "VIX"
}

BOND_DATASETS = ["AAA", "BAA", "credit_spread_baa_aaa", "DGS2", "DGS10"]

DATASETS = [
    "AAA", "BAA", "credit_spread_baa_aaa", "DGS2", "DGS10", 
    "yield_curve_10y_2y_spread", "^VIX_1day_20231027_20251027", "SPY_1min_20231027_20251027"
]

def get_missing_business_dates(df: pd.DataFrame, freq: str):
    df_copy = df.copy()
    df_copy['date'] = pd.to_datetime(df_copy['date'], utc=True).dt.normalize().dt.tz_localize(None)
    df_copy = df_copy.set_index('date').sort_index()
    
    bday_index = pd.date_range(start=df_copy.index.min(), end=df_copy.index.max(), freq=freq)
    missing = bday_index.difference(df_copy.index)
    return missing.tolist()

def get_null_dates(df: pd.DataFrame):
    return df[df.isnull().any(axis=1)]['date'].tolist()

if __name__ == "__main__":
    date_reasons = {
        "2024-03-29": "Good Friday",
        "2025-01-09": "National Day of Mourning (ex president Jimmy Carter)",
        "2025-04-18": "Good Friday"
    }
    note = {
        "2024-11-11": "Bond market closed for Veterans Day (ffill applied)",
        "2025-01-09": "There is dirty data in dataset Curve 10y-2y",
        "2025-10-13": "Bond market closed for Columbus Day (ffill applied)",
    }
    
    missing_dates_dict = {}
    
    for name in DATASETS:
        df = pd.read_csv(f"data/raw/{name}.csv")
        df_missing_dates = []
        
        if name not in BOND_DATASETS:
            if name not in ["yield_curve_10y_2y_spread"]:
                df = df.rename(columns={"caldt": "date"})
                df_missing_dates.extend(get_missing_business_dates(df, "B"))
            else:
                df_missing_dates.extend(get_null_dates(df))
            df_missing_dates.extend(get_missing_business_dates(df, US_BD))
        
            display_name = DATASET_NAMES.get(name, name)
            
            for date in df_missing_dates:
                date_str = date if isinstance(date, str) else pd.to_datetime(date).strftime('%Y-%m-%d')
                missing_dates_dict.setdefault(date_str, []).append(display_name)
    
    # Remove duplicates and sort
    for date_str in missing_dates_dict:
        missing_dates_dict[date_str] = sorted(set(missing_dates_dict[date_str]))
    
    all_missing_dates = [
        {
            "missing_date": date_str,
            "datasets": ", ".join(datasets),
            "note": note.get(date_str, "Market Closed"),
            "reason": date_reasons.get(date_str, "")
        }
        for date_str, datasets in missing_dates_dict.items()
    ]
    
    df_missing = pd.DataFrame(all_missing_dates).sort_values(by=["missing_date"])
    df_missing.to_csv("reports/integrity_report.csv", index=False)
