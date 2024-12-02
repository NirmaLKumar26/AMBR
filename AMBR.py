import os
import pandas as pd
from tqdm import tqdm
import logging
import time
import sys

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define paths
base_path = r'/home/container/'
upload_path = os.path.join(base_path, 'Upload')
output_path = os.path.join(base_path, 'Output')
OLD_path = os.path.join(base_path, 'OLD_DATA')

# Paths for master sheets
old_master_sheet_path = os.path.join(OLD_path, 'OLD_Label_and_NonLabel_Vendors_Updated.xlsx')
new_master_sheet_path = os.path.join(upload_path, '3rd-Party-Orders-Mastersheet.xlsx')

print("Unshipped Orders Automated By")
print("███╗░░██╗██╗██████╗░███╗░░░███╗░█████╗░██╗░░░░░\n"
      "████╗░██║██║██╔══██╗████╗░████║██╔══██╗██║░░░░░\n"
      "██╔██╗██║██║██████╔╝██╔████╔██║███████║██║░░░░░\n"
      "██║╚████║██║██╔══██╗██║╚██╔╝██║██╔══██║██║░░░░░\n"
      "██║░╚███║██║██║░░██║██║░╚═╝░██║██║░░██║███████╗\n"
      "╚═╝░░╚══╝╚═╝╚═╝░░╚═╝╚═╝░░░░░╚═╝╚═╝░░╚═╝╚══════╝")

# Log process start
logging.info("Starting the unshipped orders process...")

# Load the Old and New Master sheets
logging.info("Loading old master sheet (updated)...")
try:
    old_master_sheet = pd.ExcelFile(old_master_sheet_path)
    logging.info("Old master sheet (updated) loaded successfully.")
except FileNotFoundError as e:
    logging.error(f"Error: {e}")
    sys.exit()

logging.info("Loading new master sheet...")
try:
    new_master_sheet = pd.ExcelFile(new_master_sheet_path)
    logging.info("New master sheet loaded successfully.")
except FileNotFoundError as e:
    logging.error(f"Error: {e}")
    sys.exit()

# Load 'Overall vendors' sheet from new master sheet
logging.info("Loading 'Overall vendors' sheet from new master sheet...")
try:
    overall_vendors_df = pd.read_excel(new_master_sheet_path, sheet_name='Overall vendors')
    overall_vendors_df.columns = overall_vendors_df.columns.str.strip().str.lower().str.replace(' ', '_')
    logging.info("'Overall vendors' sheet loaded successfully.")
except Exception as e:
    logging.error(f"Error: {e}")
    sys.exit()

# Find the first .txt file for unshipped orders
logging.info("Searching for a .txt file in the Upload folder...")
txt_files = [f for f in os.listdir(upload_path) if f.endswith('.txt')]

if not txt_files:
    logging.error("Error: No .txt file found in the Upload folder.")
    sys.exit()

unshipped_orders_path = os.path.join(upload_path, txt_files[0])
logging.info(f"Found .txt file: {txt_files[0]}")

# Load the unshipped orders
logging.info("Loading unshipped orders TSV file...")
try:
    unshipped_orders = pd.read_csv(unshipped_orders_path, delimiter='\t')
    logging.info("Unshipped orders loaded successfully.")
except FileNotFoundError as e:
    logging.error(f"Error: {e}")
    sys.exit()

# Clean up unshipped orders and remove duplicates
logging.info("Cleaning and removing duplicates from unshipped orders...")
unshipped_orders.columns = unshipped_orders.columns.str.strip().str.lower().str.replace(' ', '_')
unshipped_orders['order-id'] = unshipped_orders['order-id'].astype(str)
unshipped_orders.drop_duplicates(subset=['order-id'], inplace=True)
logging.info("Duplicates removed.")

# Remove rows where 'sku' contains 'RET' or 'INV'
if 'sku' in unshipped_orders.columns:
    logging.info("Removing rows with 'RET' or 'INV' in SKU...")
    unshipped_orders = unshipped_orders[~unshipped_orders['sku'].str.contains('RET|INV', na=False)]
    logging.info("Unwanted SKUs removed.")

# Extract vendor names based on SKU Prefix
logging.info("Extracting vendor names from SKUs...")
unshipped_orders['vendor_name'] = unshipped_orders['sku'].apply(lambda x: x.split('-')[0] if pd.notnull(x) else '')
logging.info("Vendor names extracted.")

# Initialize dataframes for reports
label_vendors_orders = pd.DataFrame(columns=unshipped_orders.columns)
non_label_vendors_orders = pd.DataFrame(columns=unshipped_orders.columns)
unknown_vendors_orders = pd.DataFrame(columns=unshipped_orders.columns)
new_skus_orders = pd.DataFrame(columns=unshipped_orders.columns)  # DataFrame for new SKUs orders

# Define the duplicate checking function without vendor_name filtering
def check_duplicates(vendor_orders, master_sheet_path, sheet_name):
    num_duplicates = 0
    try:
        df = pd.read_excel(master_sheet_path, sheet_name=sheet_name)
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
        if 'order_id' in df.columns:
            df['order_id'] = df['order_id'].astype(str)
            duplicates = vendor_orders[vendor_orders['order-id'].isin(df['order_id'])]
            num_duplicates = len(duplicates)
            if num_duplicates > 0:
                vendor_orders = vendor_orders[~vendor_orders['order-id'].isin(duplicates['order-id'])]
        else:
            logging.error(f"Column 'order_id' not found in sheet '{sheet_name}'")
    except Exception as e:
        logging.error(f"Error reading sheet '{sheet_name}': {e}")
    return vendor_orders, num_duplicates

# Start processing each vendor
logging.info("Starting vendor-by-vendor processing...")
processed_vendors = set()

for vendor_name in tqdm(unshipped_orders['vendor_name'].unique(), desc="Processing Vendors"):
    logging.info(f"Processing vendor: {vendor_name}")
    logging.info(f"Checking for {vendor_name} in old master sheet...")

    vendor_orders = unshipped_orders[unshipped_orders['vendor_name'] == vendor_name]

    # Get label type from Overall Vendors sheet
    vendor_info = overall_vendors_df[overall_vendors_df['prefix'] == vendor_name]
    if not vendor_info.empty:
        label_type = vendor_info['label'].values[0]
    else:
        label_type = 'Unknown'

    # Log the vendor label type
    logging.info(f"Vendor {vendor_name} is labeled as {label_type}")

    if label_type != 'Unknown':
        # Check duplicates in old master sheet
        vendor_orders, num_duplicates_old = check_duplicates(
            vendor_orders,
            old_master_sheet_path,
            label_type
        )
    else:
        logging.info(f"No label type found for {vendor_name} in overall vendors sheet.")
        logging.info("Skipping duplicate check in old master sheet.")
        num_duplicates_old = 0

    if num_duplicates_old > 0:
        logging.info(f"Duplicates found for {vendor_name} in OLD Sheet: {num_duplicates_old}")
    else:
        logging.info(f"No duplicates found for {vendor_name} in OLD Sheet.")

    # Log: Checking for duplicates in {label_type} sheet in new master sheet...
    if label_type in new_master_sheet.sheet_names:
        logging.info(f"Checking for duplicates in {label_type} sheet in new master sheet...")
        vendor_orders, num_duplicates_new = check_duplicates(
            vendor_orders,
            new_master_sheet_path,
            label_type
        )
        if num_duplicates_new > 0:
            logging.info(f"Duplicates found for {vendor_name} in {label_type} Sheet: {num_duplicates_new}")
        else:
            logging.info(f"No duplicates found for {vendor_name} in {label_type} Sheet.")
    else:
        logging.warning(f"'{label_type}' sheet not found in new master sheet.")
        logging.info("0 Duplicates Found")
        num_duplicates_new = 0

    # Remove duplicates from unshipped_orders
    duplicates_order_ids = set(unshipped_orders[unshipped_orders['vendor_name'] == vendor_name]['order-id']) - set(vendor_orders['order-id'])
    if duplicates_order_ids:
        unshipped_orders = unshipped_orders[~((unshipped_orders['vendor_name'] == vendor_name) & (unshipped_orders['order-id'].isin(duplicates_order_ids)))]

    # Count new SKUs and collect orders with new SKUs
    existing_skus = set()
    if label_type != 'Unknown':
        # Read SKUs from both old and new master sheets
        skus_in_old = set()
        try:
            df_old = pd.read_excel(old_master_sheet_path, sheet_name=label_type)
            df_old.columns = df_old.columns.str.strip().str.lower().str.replace(' ', '_')
            if 'sku' in df_old.columns:
                skus_in_old = set(df_old['sku'].astype(str).unique())
            else:
                logging.warning(f"'sku' column not found in sheet '{label_type}' of old master sheet.")
        except Exception as e:
            logging.error(f"Error reading SKUs from old master sheet: {e}")

        skus_in_new = set()
        try:
            df_new = pd.read_excel(new_master_sheet_path, sheet_name=label_type)
            df_new.columns = df_new.columns.str.strip().str.lower().str.replace(' ', '_')
            if 'sku' in df_new.columns:
                skus_in_new = set(df_new['sku'].astype(str).unique())
            else:
                logging.warning(f"'sku' column not found in sheet '{label_type}' of new master sheet.")
        except Exception as e:
            logging.error(f"Error reading SKUs from new master sheet: {e}")

        existing_skus = skus_in_old.union(skus_in_new)

        new_skus = set(vendor_orders['sku'].astype(str).unique()) - existing_skus
        num_new_skus = len(new_skus)
        logging.info(f"New SKU Found for {vendor_name} - {num_new_skus}")

        # Collect orders with new SKUs, excluding unknown vendors
        if num_new_skus > 0:
            orders_with_new_skus = vendor_orders[vendor_orders['sku'].isin(new_skus)]
            new_skus_orders = pd.concat([new_skus_orders, orders_with_new_skus], ignore_index=True)
    else:
        logging.info(f"Vendor {vendor_name} is Unknown. Skipping new SKU collection.")
        num_new_skus = 0

    # Number of new orders
    num_new_orders = vendor_orders['order-id'].nunique()
    logging.info(f"New Order Found For {vendor_name} - {num_new_orders} - {label_type}")

    # Now, process vendor_orders
    if label_type == 'Label Vendors':
        label_vendors_orders = pd.concat([label_vendors_orders, vendor_orders])
        logging.info(f"New Order Details has been saved in Label Vendors report")
    elif label_type == 'Non-Label Vendors':
        non_label_vendors_orders = pd.concat([non_label_vendors_orders, vendor_orders])
        logging.info(f"New Order Details has been saved in Non-Label Vendors report")
    else:
        unknown_vendors_orders = pd.concat([unknown_vendors_orders, vendor_orders])
        logging.warning(f"Orders processed for Unknown Vendor: {vendor_name}")

# Generate Vendor Order Counts
logging.info("Generating vendor order counts...")
all_processed_orders = pd.concat([label_vendors_orders, non_label_vendors_orders], ignore_index=True)
vendor_order_counts = all_processed_orders.groupby('vendor_name')['order-id'].nunique().reset_index()
vendor_order_counts.columns = ['Vendor', 'Order Count']

# Generate SKU counts, excluding unknown vendors
logging.info("Generating SKU counts, excluding unknown vendors...")
sku_counts_df = all_processed_orders['sku'].value_counts().reset_index()
sku_counts_df.columns = ['SKU', 'Unshipped Orders']

# Create counts dataframe
label_vendors_count = label_vendors_orders['order-id'].nunique()
non_label_vendors_count = non_label_vendors_orders['order-id'].nunique()
total_order_count = all_processed_orders['order-id'].nunique()

counts_data = {
    'Order Type': ['Label Vendors Orders', 'Non-Label Vendors Orders', 'Total Orders'],
    'Order Count': [label_vendors_count, non_label_vendors_count, total_order_count]
}
counts_df = pd.DataFrame(counts_data)

# Log total counts
logging.info(f"Total Label Vendors Orders: {label_vendors_count}")
logging.info(f"Total Non-Label Vendors Orders: {non_label_vendors_count}")
logging.info(f"Total Orders: {total_order_count}")

# Prepare New SKU Report
if not new_skus_orders.empty:
    new_skus_orders.drop_duplicates(inplace=True)
    logging.info("New SKU Report has been generated.")
else:
    logging.info("No new SKUs found. New SKU Report is empty.")

# Save output reports
output_file_path = os.path.join(output_path, 'Unshipped_Report.xlsx')
logging.info(f"Saving all reports to {output_file_path}...")

with pd.ExcelWriter(output_file_path, engine='xlsxwriter') as writer:
    label_vendors_orders.to_excel(writer, sheet_name='Label_Vendors_Orders', index=False)
    non_label_vendors_orders.to_excel(writer, sheet_name='Non_Label_Vendors_Orders', index=False)
    unknown_vendors_orders.to_excel(writer, sheet_name='Unknown_Vendors_Report', index=False)
    sku_counts_df.to_excel(writer, sheet_name='SKU_Counts_Report', index=False)
    counts_df.to_excel(writer, sheet_name='Order_Counts', index=False)
    vendor_order_counts.to_excel(writer, sheet_name='Vendor_Order_Counts', index=False)
    new_skus_orders.to_excel(writer, sheet_name='New_SKU_Report', index=False)

logging.info("Processing complete. All reports have been saved.")
time.sleep(3600)
sys.exit()
