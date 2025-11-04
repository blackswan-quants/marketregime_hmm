import pandas as pd
import numpy as np
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay

US_BD = CustomBusinessDay(calendar=USFederalHolidayCalendar())  # FRED/US markets

def missing_dates(df: pd.DataFrame):
    df['date'] = pd.to_datetime(df['date'], utc=True).dt.normalize().dt.tz_localize(None)
    df = df.set_index('date').sort_index()

    bday_index = pd.date_range(start=df.index.min(), end=df.index.max(), freq=US_BD)

    # Identify missing dates
    missing = bday_index.difference(df.index)
    return missing.tolist()

if __name__ == "__main__":
    date_reasons = {
        "2024-03-29": "Good Friday",
        "2025-01-09": "National Day of Mourning (ex president Jimmy Carter)",
        "2025-04-18": "Good Friday"
    }
    
    # List to accumulate all missing dates
    all_missing_dates = []
    
    # Insert in a data frame all the missing dates
    for name in ["AAA", "BAA", "credit_spread_baa_aaa", "DGS2", "DGS10", "yield_curve_10y_2y_spread", "^VIX_1day_20231027_20251027", "SPY_1min_20231027_20251027"]:
        df = pd.read_csv(f"data/raw/{name}.csv")
        df_missing_dates = []
        
        if name not in ["AAA", "BAA", "credit_spread_baa_aaa", "DGS2", "DGS10"]:
            if name not in ["yield_curve_10y_2y_spread"]:
                df.rename(columns={"caldt": "date"}, inplace=True)
            null_values_dates = missing_dates(df)
            df_missing_dates = null_values_dates

        # Add the dates with the file name
        for date in df_missing_dates:
            if isinstance(date, str):
                date_str = date
            else:
                date_str = pd.to_datetime(date).strftime('%Y-%m-%d')
            
            reason = date_reasons.get(date_str, "")
            
            all_missing_dates.append({
                "missing_dates_SPY_and_VIX": date_str,
                "reason": reason
            })

    df_missing = pd.DataFrame(all_missing_dates)
    df_missing = df_missing.drop_duplicates()
    df_missing = df_missing.sort_values(by=["missing_dates_SPY_and_VIX"])
    df_missing.to_csv("reports/integrity_report.csv", index=False)
