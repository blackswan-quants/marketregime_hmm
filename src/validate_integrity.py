import pandas as pd
import numpy as np
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay

US_BD = CustomBusinessDay(calendar=USFederalHolidayCalendar())  # FRED/US markets

def get_missing_business_dates_US_BD(df: pd.DataFrame):
    df['date'] = pd.to_datetime(df['date'], utc=True).dt.normalize().dt.tz_localize(None)
    df = df.set_index('date').sort_index()

    bday_index = pd.date_range(start=df.index.min(), end=df.index.max(), freq=US_BD)

    # Identify missing dates
    missing = bday_index.difference(df.index)
    return missing.tolist()

def get_missing_business_dates_B(df: pd.DataFrame):
    df['date'] = pd.to_datetime(df['date'], utc=True).dt.normalize().dt.tz_localize(None)
    df = df.set_index('date').sort_index()

    bday_index = pd.date_range(start=df.index.min(), end=df.index.max(), freq="B")

    # Identify missing dates
    missing = bday_index.difference(df.index)
    return missing.tolist()

def get_missing_business_dates_null(df: pd.DataFrame):
    missing = df[df.isnull().any(axis=1)]['date']
    return missing.tolist()

if __name__ == "__main__":
    date_reasons = {
        "2024-03-29": "Good Friday",
        "2025-01-09": "National Day of Mourning (ex president Jimmy Carter)",
        "2025-04-18": "Good Friday"
    }
    note = {
        "2024-11-11": "We will apply ffill to fill this missing value",
        "2025-01-09": "There is dirty data in dataset Curve 10y-2y"
    }
    
    missing_dates_dict = {}
    
    # Insert in a data frame all the missing dates
    for name in ["AAA", "BAA", "credit_spread_baa_aaa", "DGS2", "DGS10", "yield_curve_10y_2y_spread", "^VIX_1day_20231027_20251027", "SPY_1min_20231027_20251027"]:
        df = pd.read_csv(f"data/raw/{name}.csv")
        df_missing_dates = []
        
        if name not in ["AAA", "BAA", "credit_spread_baa_aaa", "DGS2", "DGS10"]:
            if name not in ["yield_curve_10y_2y_spread"]:
                df.rename(columns={"caldt": "date"}, inplace=True)
                df_missing_dates.extend(get_missing_business_dates_B(df))
            else:
                df_missing_dates.extend(get_missing_business_dates_null(df))
            df_missing_dates.extend(get_missing_business_dates_US_BD(df))

        for date in df_missing_dates:
            if isinstance(date, str):
                date_str = date
            else:
                date_str = pd.to_datetime(date).strftime('%Y-%m-%d')

            if date_str not in missing_dates_dict:  # First time we see this date
                missing_dates_dict[date_str] = []
            
            if name == "SPY_1min_20231027_20251027":
                name = "SPY"
            elif name == "yield_curve_10y_2y_spread":
                name = "Curve 10y-2y"
            elif name == "^VIX_1day_20231027_20251027":
                name = "VIX"

            missing_dates_dict[date_str].append(name)

    # Remove duplicates from datasets for each date and sort them
    for date_str in missing_dates_dict:
        missing_dates_dict[date_str] = sorted(list(set(missing_dates_dict[date_str])))
    
    # Create the final list with the aggregated information
    all_missing_dates = []
    for date_str, datasets in missing_dates_dict.items():
        reason = date_reasons.get(date_str, "")
        note_text = note.get(date_str, "Market Closed")
        datasets_str = ", ".join(datasets)  # Join the dataset names with a comma
        
        all_missing_dates.append({
            "missing_date": date_str,
            "datasets": datasets_str,
            "note": note_text,
            "reason": reason
        })

    df_missing = pd.DataFrame(all_missing_dates)
    df_missing = df_missing.sort_values(by=["missing_date"])
    df_missing.to_csv("reports/integrity_report.csv", index=False)
