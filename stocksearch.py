from currency_converter import CurrencyConverter, RateNotFoundError
import pandas as pd
import yfinance as yf
import os
from datetime import datetime, timedelta, date

pd.set_option('display.max_columns', None)  # None means unlimited

"""The purpose of this file is to create pricedata_df: 
--> a dataframe that consists of stock and etf prices found both on the web and in the transaction files
--> in calculation.py, this data will be used, together with the transaction file to keep track of the balance of stocks
"""
def load_or_create_isin_lists(transaction_file,splits_info_df,username):
    found_path= f'found_isins_{username}.csv'
    not_found_path = f'not_found_isins_{username}.csv'

    if os.path.exists(found_path) and os.path.exists(not_found_path):
        found_df = pd.read_csv(found_path)
        not_found_df = pd.read_csv(not_found_path)
    else:
        found_df, not_found_df = create_isin_lists(transaction_file,splits_info_df)
        # Save results for future use
        found_df.to_csv(found_path, index=False)
        not_found_df.to_csv(not_found_path, index=False)
    return found_df, not_found_df
def create_isin_lists(transaction_file,splits_info_df):
    # Create a dictionary mapping ISINs to the first non-null Security Name found for each
    found_list = []
    not_found_list = []

    if splits_info_df.empty:
        print("No stock splits detected.")
    else:

        isin_to_name = transaction_file.dropna(subset=['Security Name']).groupby('ISIN')['Security Name'].first().to_dict()

        # creat isin list
        unique_isins = transaction_file['ISIN'].unique()

        # Prepare a list to hold the results

        split_isins = splits_info_df['ISIN'].unique()  # Get unique ISINs with stock splits


        # Loop through each ISIN
        for isin in unique_isins:
            security_name = isin_to_name.get(isin, "Unknown")  # Get the security name or default to "Unknown"
            # Skip processing if ISIN is NaN
            if pd.isnull(isin):
                continue

            if isin in split_isins:
                # ISIN has stock splits, add it to not_found_list
                not_found_list.append({'ISIN': isin, 'Error': 'Stock split detected', 'Security Name': security_name})
                continue
            try:
                stock = yf.Ticker(isin)
                info = stock.info
                # Check if 'shortName' and 'symbol' are present in the info dictionary
                if 'shortName' in info and 'symbol' in info:
                    found_list.append({'ISIN': isin, 'shortName': info['shortName'], 'symbol': info['symbol'], 'Security Name': security_name})
                else:
                    not_found_list.append({'ISIN': isin, 'Error': 'Information not found or ISIN not supported', 'Security Name': security_name})
            except Exception as e:
                not_found_list.append({'ISIN': isin, 'Error': str(e), 'Security Name': security_name})

    # Convert the results list to a DataFrame and print or save it
    found_df = pd.DataFrame(found_list)
    not_found_df = pd.DataFrame(not_found_list)

    # Return both DataFrames
    return found_df, not_found_df

def check_stock_splits(transaction_file):
    # creat isin list
    isin_list = transaction_file['ISIN'].unique()
    splits_info = []
    transaction_file['Booking Date'] = pd.to_datetime(transaction_file['Booking Date'])
    min_date = transaction_file['Booking Date'].min()

    for isin in isin_list:
        # Skip processing if ISIN is NaN
        if pd.isnull(isin):
            continue

        try:
            tickerinfo = yf.Ticker(isin)
            # Get historical market data
            hist = tickerinfo.history(start=min_date)

            # Check if 'Stock Splits' column exists in the DataFrame
            if 'Stock Splits' in hist.columns:
                stock_splits_df = hist[hist['Stock Splits'] != 0]

                # Check if the filtered DataFrame is not empty
                if not stock_splits_df.empty:
                    # Get the dates where stock splits occurred
                    split_dates = stock_splits_df.index
                #    print(f"Stock splits for {isin} occurred on the following dates:")
                    for date in split_dates:
                 #       print(date.date())  # Print only the date part
                        splits_info.append({'ISIN': isin, 'Stock Split Date': date.date()})
                else:
                    None
            else:
                print(f"No 'Stock Splits' data available for {isin}.")
        except Exception as e:
            print(f"Error processing {isin}: {e}")
    # Convert the list of dictionaries into a DataFrame
    splits_info_df = pd.DataFrame(splits_info)
    return splits_info_df

def create_daterange_transactions(csv_file):
    # Ensure the 'Booking date' column is in datetime format
    csv_file['Booking Date'] = pd.to_datetime(csv_file['Booking Date'])
    # Extract the minimum and maximum dates
    min_date = csv_file['Booking Date'].min()
    max_date = csv_file['Booking Date'].max()

    min_date = pd.to_datetime(min_date)
    max_date = pd.to_datetime(max_date)

    # Initialize the list with the minimum date
    dates_list = [min_date]

    # Generate year-end dates between min and max dates
    current_year = min_date.year
    while current_year < max_date.year:
        year_end_date = datetime(current_year, 12, 31)
        if min_date < year_end_date < max_date:
            dates_list.append(year_end_date)
        current_year += 1

    # Add the maximum date to the list if it's not already included
    if dates_list[-1] != max_date:
        dates_list.append(max_date)

    return dates_list


def price_by_date(isin,securityname,ticker, date, dataframe):
    c = CurrencyConverter(fallback_on_missing_rate=True, fallback_on_wrong_date=True)
    # Now 'date' is expected to be a datetime.date object, no conversion needed
    date1 = date - timedelta(days=4)
    date2 = date + timedelta(days=1)

    date1_str = date1.strftime("%Y-%m-%d")
    date2_str = date2.strftime("%Y-%m-%d")

    stock = yf.Ticker(ticker)
    currency = stock.info['currency']
    info_by_date = stock.history(start=date1_str, end=date2_str)

    if not info_by_date.empty:
        last_close_price = info_by_date['Close'].iloc[-1]
        last_date = info_by_date.index[-1].date()

        if currency != 'EUR':
            try:
                last_close_price = c.convert(last_close_price, currency, 'EUR', date=last_date)
            except RateNotFoundError:
                last_close_price = c.convert(last_close_price, currency, 'EUR') # use latest available rate

        new_row = pd.DataFrame({
            'ISIN': isin,  # Use ISIN here
            'Security Name' : securityname,
            'Price Quote': [last_close_price],
            'Date of Price Quote': [pd.to_datetime(last_date)],  # Convert string to datetime
            'Currency': ['EUR' if currency != 'EUR' else currency],
            'Added from': ['Web']
        })

        dataframe = pd.concat([dataframe, new_row], ignore_index=True)
    else:
        print(f"Error: No data found for {ticker} on the specified range.")

    return dataframe


def price_by_transactions(transaction_file,pricedata_df):
    # Add a new column for Price Quote
    transaction_file['Price Quote'] = None  # Initialize with None
    # Calculate Price Quote for 'Kauf' and 'Verkauf' transactions
    for index, row in transaction_file.iterrows():
        if row['Transaction Type'] == 'Kauf':
            try:
                # Add a minus sign for 'Kauf'
                transaction_file.at[index, 'Price Quote'] = -(row['Transaction Value'] / row['Security Amount'])
            except ZeroDivisionError:
                transaction_file.at[index, 'Price Quote'] = None  # Handle division by zero if Security Amount is 0
        elif row['Transaction Type'] == 'Verkauf':
            try:
                transaction_file.at[index, 'Price Quote'] = row['Transaction Value'] / row['Security Amount']
            except ZeroDivisionError:
                transaction_file.at[index, 'Price Quote'] = None
    # Filter entries with a calculated Price Quote
    valid_entries = transaction_file.dropna(subset=['Price Quote'])
    print(valid_entries)

    # Prepare these entries to be added to pricedata_df
    new_entries = valid_entries[['ISIN','Security Name','Price Quote', 'Booking Date']].copy()
    new_entries.rename(columns={'Booking Date': 'Date of Price Quote'}, inplace=True)
    new_entries['Currency'] = 'EUR'  # Add Currency column
    new_entries['Added from'] = 'Transactions'  # Add Added from column

    # Assuming pricedata_df is initialized somewhere in your script
    pricedata_df = pd.concat([pricedata_df, new_entries], ignore_index=True)

    return pricedata_df

