import pandas as pd
import os
import pdfextract
import stocksearch
import calculation



#pdfextract workflow

# Main workflow
# Define the new current working directory
new_cwd = r'C:\Users\...' # fill in your working directory here
# Change the current working directory
os.chdir(new_cwd)
folder_path = r'C:\Users\...' # fill in the folder with your pdfs
username = "John Doe" # fill in your username here



#pdfextract workflow
pdfextract.process_folder_of_pdfs(folder_path,username)


#stocksearch workflow
# read the CSV file
transaction_file = pd.read_csv(f'transactions_handover_{username}.csv')
# generate stock split info
splits_info_df = stocksearch.check_stock_splits(transaction_file)
# Generate found and not found ISIN lists
found_df, not_found_df = stocksearch.load_or_create_isin_lists(transaction_file,splits_info_df,username)
# Initialize an empty DataFrame outside the function
pricedata_df = pd.DataFrame(columns=['ISIN','Security Name', 'Price Quote', 'Date of Price Quote', 'Currency', 'Added from'])
pricedata_df['Date of Price Quote'] = pd.to_datetime(pricedata_df['Date of Price Quote']).dt.date
# Generate the list of dates
dates_list = stocksearch.create_daterange_transactions(transaction_file)  # Assuming this function is defined

# Loop through each ISIN in the found list and fetch prices
for _, row in found_df.iterrows():
    isin = row['ISIN']  # Get the ISIN
    ticker = row['symbol']
    securityname = row['Security Name']
    for d in dates_list:
        pricedata_df = stocksearch.price_by_date(isin,securityname, ticker, d, pricedata_df)
# add transaction prices
pricedata_df = stocksearch.price_by_transactions(transaction_file, pricedata_df)
#After  updating pricedata_df, you might want to save it
pricedata_excelfilename = f'pricedata_updated_{username}.xlsx'
pricedata_csvfilename = f'pricedata_updated_{username}.csv'
pricedata_df.to_csv(pricedata_csvfilename, index=False)
pricedata_df.to_excel(pricedata_excelfilename, index=False)
pricedata_file= pd.read_csv(f'pricedata_updated_{username}.csv')

#calc workflow

excel_summary_filename = f'transaction_summary_{username}.xlsx'

#create overview table
overview_table = []

# Loop through the dates_list in pairs
for i in range(len(dates_list) - 1):
    beginning_of_period_date = dates_list[i]
    end_of_period_date = dates_list[i + 1]
    beginning_of_period_date_str = beginning_of_period_date.strftime("%Y-%m-%d")
    end_of_period_date_str = end_of_period_date.strftime("%Y-%m-%d")

    print(f"Calculating returns between {beginning_of_period_date_str} and {end_of_period_date_str}")
    # Calculate and print the summary for each period
    summary_df = calculation.calculate_returns_between_dates(beginning_of_period_date.strftime('%Y-%m-%d'),
                                                 end_of_period_date.strftime('%Y-%m-%d'),
                                                 transaction_file, pricedata_file,username)
    # Extract necessary info and append to the overview_table
    if not summary_df.empty:
        total_absolute_return_in_period = summary_df.iloc[:-1]['Absolute Return in Period'].sum()
    else:
        total_absolute_return_in_period = 0
    total_return_for_period = total_absolute_return_in_period / summary_df['Balance BoP'].sum() * 100  # Convert to percentage
    overview_table.append({
        'Beginning of Period Date': beginning_of_period_date_str,
        'End of Period Date': end_of_period_date_str,
        'Absolute Return': total_absolute_return_in_period,
        'Relative Return': total_return_for_period
    })
# Special case for Min & Max
print(f"Calculating returns between {dates_list[0]} and {dates_list[-1]}")
summary_df = calculation.calculate_returns_between_dates(dates_list[0].strftime('%Y-%m-%d'),
                                             dates_list[-1].strftime('%Y-%m-%d'),
                                             transaction_file, pricedata_file,username)

# Append the Min & Max case to overview_table
if not summary_df.empty:
    total_absolute_return_in_period = summary_df.iloc[:-1]['Absolute Return in Period'].sum()
else:
    total_absolute_return_in_period = 0
total_return_for_period = total_absolute_return_in_period / summary_df['Balance BoP'].sum() * 100  # Convert to percentage
overview_table.append({
    'Beginning of Period Date': dates_list[0].strftime('%Y-%m-%d'),
    'End of Period Date': dates_list[-1].strftime('%Y-%m-%d'),
    'Absolute Return': total_absolute_return_in_period,
    'Relative Return': total_return_for_period
})

# Convert overview_table to DataFrame and print it
final_results_df = pd.DataFrame(overview_table)
with pd.ExcelWriter(excel_summary_filename, engine='openpyxl', mode='a') as writer:
    final_results_df.to_excel(writer, sheet_name='Summary', index=False)
