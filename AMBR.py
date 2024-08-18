import os
import pandas as pd
from tqdm import tqdm
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define paths
base_path = r'C:\Users\Administrator\Desktop\Unshipped Order'
upload_path = os.path.join(base_path, 'Upload')
output_path = os.path.join(base_path, 'Output')

master_sheet_path = os.path.join(upload_path, '3rd-Party-Orders-Mastersheet.xlsx')
unshipped_orders_path = os.path.join(upload_path, 'Unshipped-Orders.xlsx')

# Read the Master sheet
logging.info("Loading master sheet...")
master_sheet = pd.ExcelFile(master_sheet_path)

# Read the Unshipped Orders file
try:
    logging.info("Loading unshipped orders...")
    unshipped_orders = pd.read_excel(unshipped_orders_path)
except FileNotFoundError as e:
    logging.error(f"Error: {e}")
    exit()

# Ensure column names do not have leading/trailing spaces
unshipped_orders.columns = unshipped_orders.columns.str.strip()

# Verify if 'order-id' column exists
if 'order-id' not in unshipped_orders.columns:
    logging.error("Error: 'order-id' column not found in Unshipped Orders file.")
    exit()

# Convert 'order-id' to string
unshipped_orders['order-id'] = unshipped_orders['order-id'].astype(str)

# Remove duplicates in Unshipped Orders based on 'order-id'
logging.info("Removing duplicates from unshipped orders...")
unshipped_orders.drop_duplicates(subset=['order-id'], inplace=True)

# Remove rows where 'sku' contains 'RET' or 'INV'
if 'sku' in unshipped_orders.columns:
    logging.info("Removing rows with 'RET' or 'INV' in SKU...")
    unshipped_orders = unshipped_orders[~unshipped_orders['sku'].str.contains('RET|INV', na=False)]

# Extract vendor names from SKUs
unshipped_orders['vendor_name'] = unshipped_orders['sku'].apply(lambda x: x.split('-')[0])

# Initialize a list to collect unprocessed orders
unprocessed_orders = []

logging.info("Processing each vendor in Unshipped Orders...")
# Process each vendor in Unshipped Orders
for vendor_name in tqdm(unshipped_orders['vendor_name'].unique(), desc="Processing Vendors"):
    if vendor_name in master_sheet.sheet_names:
        vendor_sheet = pd.read_excel(master_sheet_path, sheet_name=vendor_name)
        # Ensure column names do not have leading/trailing spaces
        vendor_sheet.columns = vendor_sheet.columns.str.strip()
        # Convert 'Order Id' to string
        vendor_sheet['Order Id'] = vendor_sheet['Order Id'].astype(str)
        # Merge with unshipped orders of this vendor
        vendor_unshipped_orders = unshipped_orders[unshipped_orders['vendor_name'] == vendor_name]
        merged_df = vendor_unshipped_orders.merge(vendor_sheet, left_on='order-id', right_on='Order Id', how='left', indicator=True)
        # Rows that exist in both Unshipped Orders and Master sheet
        duplicates = merged_df[merged_df['_merge'] == 'both']
        # Remove these rows from unshipped_orders
        unshipped_orders = unshipped_orders[~unshipped_orders['order-id'].isin(duplicates['order-id'])]
        # Log progress message
        logging.info(f"{vendor_name} Completed: {len(vendor_unshipped_orders) - len(duplicates)} Unprocessed Orders")
    else:
        unprocessed_orders.extend(unshipped_orders[unshipped_orders['vendor_name'] == vendor_name]['sku'].tolist())

# Remove unprocessed SKUs from unshipped_orders
unshipped_orders = unshipped_orders[~unshipped_orders['sku'].isin(unprocessed_orders)]

# Create a DataFrame for the unprocessed orders report
unprocessed_orders_df = pd.DataFrame(unprocessed_orders, columns=['SKU'])

# Generate a report with the count of unshipped orders per SKU
sku_counts = unshipped_orders['sku'].value_counts().reset_index()
sku_counts.columns = ['SKU', 'Unshipped Orders']

# Generate a report with the count of unprocessed orders per vendor
unprocessed_vendor_counts = pd.DataFrame(unprocessed_orders, columns=['SKU'])
unprocessed_vendor_counts['vendor_name'] = unprocessed_vendor_counts['SKU'].apply(lambda x: x.split('-')[0])
unprocessed_vendor_counts = unprocessed_vendor_counts['vendor_name'].value_counts().reset_index()
unprocessed_vendor_counts.columns = ['Vendor', 'Unprocessed Orders']

# Generate a report with the count of unshipped orders per vendor
unshipped_vendor_counts = unshipped_orders['vendor_name'].value_counts().reset_index()
unshipped_vendor_counts.columns = ['Vendor', 'Unshipped Orders']

# Save the cleaned Unshipped Orders, Unprocessed Report, SKU Count Report, and Vendor Unshipped Orders Count
unshipped_orders_output_path = os.path.join(output_path, 'cleaned_unshipped_orders.xlsx')
unprocessed_orders_output_path = os.path.join(output_path, 'unprocessed_report.xlsx')
sku_counts_output_path = os.path.join(output_path, 'sku_counts_report.xlsx')
unshipped_vendor_counts_output_path = os.path.join(output_path, 'unshipped_vendor_counts.xlsx')

logging.info(f"Saving cleaned Unshipped Orders to {unshipped_orders_output_path}...")
unshipped_orders.to_excel(unshipped_orders_output_path, index=False)
logging.info(f"Saving Unprocessed Report to {unprocessed_orders_output_path}...")
unprocessed_orders_df.to_excel(unprocessed_orders_output_path, index=False)
logging.info(f"Saving SKU Counts Report to {sku_counts_output_path}...")
sku_counts.to_excel(sku_counts_output_path, index=False)
logging.info(f"Saving Unshipped Orders Count per Vendor to {unshipped_vendor_counts_output_path}...")
unshipped_vendor_counts.to_excel(unshipped_vendor_counts_output_path, index=False)

# Log the unprocessed vendor counts
for idx, row in unprocessed_vendor_counts.iterrows():
    logging.info(f"{row['Vendor']} - {row['Unprocessed Orders']} Unprocessed Orders")

logging.info("Processing complete. Cleaned Unshipped Orders, Unprocessed Report, SKU Counts Report, and Unshipped Orders Count per Vendor have been saved.")
