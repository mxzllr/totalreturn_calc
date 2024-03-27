import pandas as pd
import os
from stocksearch import create_daterange_transactions

pd.set_option('display.max_columns', None)  # None means unlimited


"""

Manual:
create all transactions (using the existing pdfs - check file path, potentially rename?)
create the price list (potentially renmaing)
run the calculation

"""



def calculate_returns_between_dates(beginning_of_period_date_str, end_of_period_date_str, transactions_df, pricedata_df,username):
    # Standardization
    transactions_df['Booking Date'] = pd.to_datetime(transactions_df['Booking Date'])
    transactions_df['ISIN'] = transactions_df['ISIN'].str.strip().str.upper()
    pricedata_df['ISIN'] = pricedata_df['ISIN'].str.strip().str.upper()
    pricedata_df['Date of Price Quote'] = pd.to_datetime(pricedata_df['Date of Price Quote'], errors='coerce')
    transactions_df['Transaction Type'] = transactions_df['Transaction Type'].str.strip()

    # security name dataframe
    security_names_df = pricedata_df[['ISIN', 'Security Name']].drop_duplicates(subset=['ISIN'])

    # Convert string dates to datetime
    beginning_of_period_date = pd.to_datetime(beginning_of_period_date_str)
    end_of_period_date = pd.to_datetime(end_of_period_date_str)

    # Filter transactions between the beginning and end of the period dates
    filtered_transactions = transactions_df[(transactions_df['Booking Date'] >= beginning_of_period_date) &
                                            (transactions_df['Booking Date'] <= end_of_period_date)]

    # Calculate Beginning and Ending Amount for each ISIN
    transactions_before_beginning = transactions_df[transactions_df['Booking Date'] < beginning_of_period_date]
    transactions_before_ending = transactions_df[transactions_df['Booking Date'] <= end_of_period_date]


    # Perform calculations

    # a) Beginning Amount: Kauf - Verkauf for each ISIN up to the beginning of period date
    beginning_amount = transactions_before_beginning.groupby(['ISIN', 'Transaction Type'])[
        'Security Amount'].sum().unstack(fill_value=0)
    beginning_amount['Beginning Amount'] = beginning_amount.get('Kauf', 0) - beginning_amount.get('Verkauf', 0)

    # b) Ending Amount: Kauf - Verkauf for each ISIN up to the beginning of period date
    ending_amount = transactions_before_ending.groupby(['ISIN', 'Transaction Type'])[
        'Security Amount'].sum().unstack(fill_value=0)
    ending_amount['Ending Amount'] = ending_amount.get('Kauf', 0) - ending_amount.get('Verkauf', 0)

    # c) Total Purchases (Kauf) for each ISIN
    purchases = filtered_transactions[filtered_transactions['Transaction Type'] == 'Kauf'].groupby('ISIN')[
        'Transaction Value'].sum()

    # d) Total Sales (Verkauf) for each ISIN
    sales = filtered_transactions[filtered_transactions['Transaction Type'] == 'Verkauf'].groupby('ISIN')[
        'Transaction Value'].sum()

    # e) Total Dividends/Coupons received for each ISIN
    dividends_coupons = \
        filtered_transactions[filtered_transactions['Transaction Type'] == 'Coupons/Dividende'].groupby('ISIN')[
            'Transaction Value'].sum()

    # Convert Series to DataFrames for uniformity
    purchases_df = purchases.reset_index(name='Total Purchases')
    sales_df = sales.reset_index(name='Total Sales')
    dividends_coupons_df = dividends_coupons.reset_index(name='Total Dividends/Coupons')

    # Find the closest date to analysis_date for each ISIN in pricedata_df
    def get_closest_price_quotes(pricedata_df, date):
        # Ensure the date column is in datetime format
        pricedata_df['Date of Price Quote'] = pd.to_datetime(pricedata_df['Date of Price Quote'])
        # Calculate the absolute difference in days from the given date
        pricedata_df['Days from Date'] = (pricedata_df['Date of Price Quote'] - pd.to_datetime(date)).abs()
        # Find the row with the closest date for each ISIN
        closest_price_quotes = pricedata_df.sort_values(['ISIN', 'Days from Date']).groupby(
            'ISIN').first().reset_index()
        return closest_price_quotes[['ISIN', 'Price Quote', 'Days from Date']]

    closest_price_ending_period = get_closest_price_quotes(pricedata_df,end_of_period_date)
    closest_price_beginning_period = get_closest_price_quotes(pricedata_df, beginning_of_period_date)

    # Merge holdings with purchases, sales, and dividends/coupons data
    summary_df = ending_amount.merge(purchases_df, on='ISIN', how='left') \
        .merge(sales_df, on='ISIN', how='left') \
        .merge(dividends_coupons_df, on='ISIN', how='left') \
        .merge(beginning_amount, on='ISIN', how='left')


    #Rename and merge price quote columns
    summary_df = summary_df.merge(
        closest_price_ending_period[['ISIN', 'Price Quote', 'Days from Date']],
        on='ISIN',
        how='left'
    ).rename(columns={
        'Price Quote': 'Closest Price Quote Ending Period',
        'Days from Date': 'Days from Ending Date'
    })
    summary_df = summary_df.merge(
        closest_price_beginning_period[['ISIN', 'Price Quote', 'Days from Date']],
        on='ISIN',
        how='left'
    ).rename(columns={
        'Price Quote': 'Closest Price Quote Beginning Period',
        'Days from Date': 'Days from Beginning Date'
    })


    # Calculate Holdings Value by multiplying Amount by Price Quote

    summary_df['Holdings Value Beginning'] = summary_df['Beginning Amount'] * summary_df['Closest Price Quote Beginning Period']
    summary_df['Holdings Value Ending'] = summary_df['Ending Amount'] * summary_df['Closest Price Quote Ending Period']

    summary_df.fillna(0, inplace=True)

    # Calculate Return in Period
    summary_df['Balance BoP'] = summary_df['Holdings Value Beginning'] -summary_df['Total Purchases']
    summary_df['Balance EoP'] = summary_df['Holdings Value Ending']   + summary_df['Total Dividends/Coupons'] + summary_df['Total Sales']

    summary_df.fillna(0, inplace=True)

    # perform ISIN level calculations
    summary_df['Absolute Return in Period'] = summary_df['Balance EoP'] - summary_df['Balance BoP']
    summary_df['% Return in Period'] = summary_df['Absolute Return in Period'] / summary_df['Balance BoP'].replace(0, pd.NA)

    # add security name
    summary_df = summary_df.merge(security_names_df, on='ISIN', how='left')

    # perform total return calculations
    total_absolute_return_in_period = summary_df['Absolute Return in Period'].sum()
    total_balance_bop = summary_df['Balance BoP'].sum()
    total_return_for_period = total_absolute_return_in_period / total_balance_bop

    summary_row = {
        'Balance BoP': summary_df['Balance BoP'].sum(),
        'Absolute Return in Period': summary_df['Absolute Return in Period'].sum(),
        '% Return in Period': summary_df['Absolute Return in Period'].sum() / summary_df['Balance BoP'].replace(0,                                                                                                       pd.NA).sum(),
    }

    # Append the summary_row to the DataFrame
    summary_row_df = pd.DataFrame([summary_row])
    summary_df = pd.concat([summary_df, summary_row_df], ignore_index=True)

    # Reorder columns
    column_order = ['ISIN','Beginning Amount','Ending Amount','Holdings Value Beginning','Holdings Value Ending','Closest Price Quote Ending Period', 'Days from Ending Date', 'Closest Price Quote Beginning Period','Days from Beginning Date','Total Purchases', 'Total Sales',
                    'Total Dividends/Coupons','Balance BoP','Balance EoP','Absolute Return in Period', '% Return in Period']
    summary_df = summary_df[column_order]




    # Specify the Excel file path
    output_path = f'transaction_summary_{username}.xlsx'
    # Check if file exists
    if not os.path.exists(output_path):
        # If not, create a new Excel file by saving a dummy empty DataFrame
        pd.DataFrame().to_excel(output_path)

    # Assuming beginning_of_period_date and end_of_period_date are datetime objects
    beginning_of_period_date_str = beginning_of_period_date.strftime("%Y-%m-%d")
    end_of_period_date_str = end_of_period_date.strftime("%Y-%m-%d")

    # Use ExcelWriter to write to specific sheet
    sheet_name = f"{beginning_of_period_date_str} to {end_of_period_date_str} calc"
    with pd.ExcelWriter(output_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
        summary_df.to_excel(writer, sheet_name=sheet_name, index=False)
    print(f"The absolute return for the period from {beginning_of_period_date_str} until {end_of_period_date_str} is {total_absolute_return_in_period:.2f}")
    print(f"The relative return for the period from {beginning_of_period_date_str} until {end_of_period_date_str} is {total_return_for_period * 100:.2f}%")

    # Optionally return the DataFrame
    return summary_df
