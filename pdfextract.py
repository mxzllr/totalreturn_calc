import PyPDF2
import re
import pandas as pd
import os
from datetime import datetime


def extract_text_from_pdf_by_page(pdf_path):
    with open(pdf_path, 'rb') as file:
        pdf_reader = PyPDF2.PdfReader(file)
        pages_text = [page.extract_text() + '\n' for page in pdf_reader.pages]
    return pages_text

def cut_text(text):
    transaction_blocks = []
    transaction_ids = [(m.start(0), m.group(1)) for m in re.finditer(r'Vorgangs-Nr\.: (\w* ?\d+)', text)]
    for i, (pos, trans_id) in enumerate(transaction_ids):
        start_pos = 0 if i == 0 else transaction_ids[i - 1][0]
        end_pos = pos if i < len(transaction_ids) - 1 else len(text)
        trans_text = text[start_pos:end_pos]
        transaction_blocks.append((trans_id, trans_text))
    return transaction_blocks

def extract_transaction_details(trans_text, trans_id, transaction_counter):
    booking_date = re.search(r'(\d{2}\.\d{2}\.\d{4})', trans_text)
    valuta_date = re.search(r'(\d{2}\.\d{2}\.\d{4})', trans_text.split('\n')[1]) if booking_date else None
    transaction_type = re.search(r'(Kauf|Verkauf|Lastschrift aktiv|SEPA-Ueberweisung|Coupons/Dividende|Steuerausgleich|Ordergebühr|Rechnungsabschluss|Broker Fee|Promotion)', trans_text)
    transaction_value_match = re.search(r'(\d{1,3}(?:\.\d{3})*,\d{2})\s*(-)?', trans_text)
    transaction_value = None  # Define transaction_value with a default value before the conditional check
    if transaction_value_match:
        # Remove dots (thousand separators) and replace comma with dot for decimal
        formatted_value = transaction_value_match.group(1).replace('.', '').replace(',', '.')
        transaction_value = float(formatted_value)
        if transaction_value_match.group(2):  # Apply negative sign if present
            transaction_value = -transaction_value
    security_name_match = re.search(r'((?:Kauf|Verkauf|Lastschrift aktiv|SEPA-Ueberweisung|Coupons/Dividende|Steuerausgleich|Ordergebühr|Rechnungsabschluss|Broker Fee|Promotion)\s+[\d,]+\s+-?\s+(.*?))\s+ISIN', trans_text, re.DOTALL)
    isin = re.search(r'ISIN\s+(\w+)', trans_text)
    security_amount_match = re.search(r'STK\s+([\d,]+)', trans_text)



    # Fill in Valuta Date with Booking Date if Valuta Date is empty but Booking Date is not
    if booking_date and not valuta_date:
        valuta_date = booking_date    # Fill in Valuta Date with Booking Date if Valuta Date is empty but Booking Date is not


    return {
        'Transaction Number': transaction_counter,
        'Booking Date': booking_date.group(1) if booking_date else None,
        'Valuta Date': valuta_date.group(1) if valuta_date else None,
        'Transaction Type': transaction_type.group(1) if transaction_type else None,
        'Transaction Value': transaction_value,
        'Security Name': security_name_match.group(2).strip() if security_name_match else None,
        'ISIN': isin.group(1) if isin else None,
        'Security Amount': float(security_amount_match.group(1).replace(',', '.')) if security_amount_match else None,
        'Transaction ID': trans_id
    }

def extract_first_transaction_details(trans_text, trans_id, transaction_counter):
    # Search for "Übertrag" in trans_text and adjust if found
    ubertrag_pos = max(trans_text.find("Übertrag"),trans_text.find("Gutschrift"))
    if ubertrag_pos != -1:
        newline_count = 0
        for i in range(ubertrag_pos, len(trans_text)):
            if trans_text[i] == '\n':
                newline_count += 1
                if newline_count == 2:  # After finding the second newline
                    new_start_pos = i + 1
                    trans_text = trans_text[new_start_pos:]
                    break

    # Call extract_transaction_details directly with the adjusted trans_text
    details = extract_transaction_details(trans_text, trans_id, transaction_counter)
    details['Processed By'] = 'First Transaction'
    # Check if the transaction has essential data
    if details.get('Transaction Value') and details.get('Transaction Type') :
        return details
    else:
        # Return None to indicate this block should be skipped
        return None

def extract_subsequent_transaction_details(trans_text, trans_id, transaction_counter):
    # This function is intended for all transactions except the first one on each page.
    details = extract_transaction_details(trans_text, trans_id, transaction_counter)
    details['Processed By'] = 'Subsequent Transaction'  # Specific indication for subsequent transactions
    return details


def extract_transactions_from_pages(pages_text, start_transaction_number):
    transactions = []
    transaction_counter = start_transaction_number  # Use the global counter passed as a parameter
    for page_text in pages_text:
        transaction_blocks = cut_text(page_text)
        first_transaction_found = False
        for i, (trans_id, trans_text) in enumerate(transaction_blocks):
            if not first_transaction_found:
                transaction_details = extract_first_transaction_details(trans_text, trans_id, transaction_counter)
                if transaction_details is None:  # If first transaction is invalid, skip to the next one
                    continue
                first_transaction_found = True
            else:
                transaction_details = extract_subsequent_transaction_details(trans_text, trans_id, transaction_counter)

            transactions.append(transaction_details)
            transaction_counter += 1

    df_transactions = pd.DataFrame(transactions)
    return df_transactions, transaction_counter  # Return the updated counter

def process_folder_of_pdfs(folder_path,username):
    all_transactions = pd.DataFrame()
    global_transaction_counter = 1  # Initialize the global transaction counter
    for filename in os.listdir(folder_path):
        if filename.endswith(".pdf"):
            pdf_path = os.path.join(folder_path, filename)
            print(f"Processing file: {filename}")  # Print statement for each file
            pages_text = extract_text_from_pdf_by_page(pdf_path)
            df_transactions, global_transaction_counter = extract_transactions_from_pages(pages_text,global_transaction_counter)
            # Optionally, add filename or any other identifier to the DataFrame
            df_transactions['PDF Name'] = filename
            all_transactions = pd.concat([all_transactions, df_transactions], ignore_index=True)


    # Convert 'Booking Date' and 'Valuta Date' to datetime format
    all_transactions['Booking Date'] = pd.to_datetime(all_transactions['Booking Date'], format='%d.%m.%Y')
    all_transactions['Valuta Date'] = pd.to_datetime(all_transactions['Valuta Date'], format='%d.%m.%Y')

    # Construct the filenames with the username included
    excel_filename = f'all_transactions_{username}.xlsx'
    csv_filename = f'transactions_handover_{username}.csv'

    # Save the aggregated results to an Excel file
    all_transactions.to_excel(excel_filename, index=False)
     # Save the aggregated results to a CSV file
    all_transactions.to_csv(csv_filename, index=False)


