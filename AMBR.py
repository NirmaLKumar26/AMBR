import os
import pandas as pd
from multiprocessing import Pool, cpu_count
import logging
import time
import sys
from discord_webhook import DiscordWebhook, DiscordEmbed
import requests
from io import BytesIO
from datetime import datetime, timedelta
import pytz  # Added for timezone conversions
import gdown  # Added for downloading Google Sheets

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Discord webhook URL
DISCORD_WEBHOOK_URL = 'https://canary.discord.com/api/webhooks/'

# Function to send logs to Discord
def send_to_discord(message, title="Unshipped Orders Log", color=0x00FF00):
    webhook = DiscordWebhook(url=DISCORD_WEBHOOK_URL)
    embed = DiscordEmbed(title=title, description=message, color=color)
    webhook.add_embed(embed)
    webhook.execute()

# Define paths
base_path = r'/home/container/'
upload_path = os.path.join(base_path, 'Upload')
output_path = os.path.join(base_path, 'Output')
OLD_path = os.path.join(base_path, 'OLD_DATA')
Bulk_buy = os.path.join(base_path, 'Bulkbuy')
returns = os.path.join(base_path, 'Returns')


# Paths for master sheets
old_master_sheet_path = os.path.join(OLD_path, 'OLD_Label_and_NonLabel_Vendors_Updated.xlsx')

# Google Sheets link for the new master sheet
google_sheet_url = 'https://docs.google.com/spreadsheets/d/****/export?format=xlsx'

# Define the path to save the downloaded file
new_master_sheet_path = os.path.join(upload_path, '3rd-Party-Orders-Mastersheet.xlsx')

# Download the Google Sheet
logging.info("Downloading the latest version of the new master sheet...")
try:
    gdown.download(google_sheet_url, new_master_sheet_path, quiet=False)
    logging.info("Downloaded the new master sheet successfully.")
except Exception as e:
    logging.error(f"Error downloading the new master sheet: {e}")
    sys.exit()

# Log process start
logging.info("Starting the unshipped orders process...")

# Load the Old and New Master sheets
def load_excel_sheets(file_path):
    try:
        excel_file = pd.ExcelFile(file_path)
        sheets = {sheet_name: excel_file.parse(sheet_name) for sheet_name in excel_file.sheet_names}
        for sheet in sheets.values():
            sheet.columns = sheet.columns.str.strip().str.lower().str.replace(' ', '_')
        return sheets
    except FileNotFoundError as e:
        logging.error(f"Error: {e}")
        sys.exit()

logging.info("Loading old and new master sheets...")
old_master_sheets = load_excel_sheets(old_master_sheet_path)
new_master_sheets = load_excel_sheets(new_master_sheet_path)

# Load 'Overall vendors' sheet
logging.info("Loading 'Overall vendors' sheet from new master sheet...")
overall_vendors_df = new_master_sheets.get('Overall vendors')
if overall_vendors_df is None:
    logging.error("'Overall vendors' sheet not found in new master sheet.")
    sys.exit()

# Clean column names
overall_vendors_df.columns = overall_vendors_df.columns.str.strip().str.lower().str.replace(' ', '_')

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
unshipped_orders = pd.read_csv(unshipped_orders_path, delimiter='\t')
unshipped_orders.columns = unshipped_orders.columns.str.strip().str.lower().str.replace(' ', '_')

# Remove rows where 'sku' contains 'RET' or 'INV' and store them separately
if 'sku' in unshipped_orders.columns:
    logging.info("Removing rows with 'RET' or 'INV' in SKU...")

    # Store removed rows
    removed_orders = unshipped_orders[unshipped_orders['sku'].str.contains('-RET|-INV|-ret', na=False)].copy()

    # Keep only valid orders
    unshipped_orders = unshipped_orders[~unshipped_orders['sku'].str.contains('-RET|-INV|-ret', na=False)]

# Extract vendor names based on SKU Prefix
logging.info("Extracting vendor names from SKUs...")
unshipped_orders['vendor_name'] = unshipped_orders['sku'].str.split('-').str[0]

# Remove 'Unknown Vendors'
logging.info("Filtering out 'Unknown Vendors'...")
unshipped_orders = unshipped_orders[~unshipped_orders['vendor_name'].isin(['Unknown'])]

# Preload existing SKUs for faster lookup
def preload_skus(master_sheets):
    skus = {}
    for sheet_name, df in master_sheets.items():
        if 'sku' in df.columns:
            skus[sheet_name] = set(df['sku'].dropna().astype(str).unique())
    return skus

logging.info("Preloading SKUs from master sheets...")
old_skus = preload_skus(old_master_sheets)
new_skus = preload_skus(new_master_sheets)

# Define processing function for each vendor
def process_vendor(vendor_name):
    logging.info(f"Processing vendor: {vendor_name}")
    vendor_orders = unshipped_orders[unshipped_orders['vendor_name'] == vendor_name]

    # Check if vendor_orders is empty
    if vendor_orders.empty:
        logging.warning(f"No orders found for vendor: {vendor_name}")
        return pd.DataFrame(), 'Unknown'

    # Determine label type
    vendor_info = overall_vendors_df[overall_vendors_df['prefix'] == vendor_name]
    label_type = vendor_info['label'].values[0] if not vendor_info.empty else 'Unknown'

    # Remove duplicates
    if label_type in old_master_sheets:
        old_orders = old_master_sheets[label_type]['order_id'].astype(str)
        vendor_orders = vendor_orders[~vendor_orders['order-id'].isin(old_orders)]
    if label_type in new_master_sheets:
        new_orders = new_master_sheets[label_type]['order_id'].astype(str)
        vendor_orders = vendor_orders[~vendor_orders['order-id'].isin(new_orders)]

    # Add new_sku column (default to False)
    vendor_orders = vendor_orders.copy()  # Avoid SettingWithCopyWarning
    vendor_orders['new_sku'] = False  # Initialize with False
    if label_type != 'Unknown':
        existing_skus = old_skus.get(label_type, set()).union(new_skus.get(label_type, set()))
        vendor_orders['new_sku'] = ~vendor_orders['sku'].isin(existing_skus)

    return vendor_orders, label_type

# Process vendors in parallel
logging.info("Starting parallel processing for vendors...")
vendor_names = unshipped_orders['vendor_name'].unique()

with Pool(cpu_count()) as pool:
    results = pool.map(process_vendor, vendor_names)

# Combine results
label_vendors_orders = pd.DataFrame()
non_label_vendors_orders = pd.DataFrame()
unknown_vendors_orders = pd.DataFrame()
new_skus_orders = pd.DataFrame()

for vendor_orders, label_type in results:
    if label_type == 'Label Vendors':
        label_vendors_orders = pd.concat([label_vendors_orders, vendor_orders], ignore_index=True)
    elif label_type == 'Non-Label Vendors':
        non_label_vendors_orders = pd.concat([non_label_vendors_orders, vendor_orders], ignore_index=True)
    else:
        unknown_vendors_orders = pd.concat([unknown_vendors_orders, vendor_orders], ignore_index=True)
    if 'new_sku' in vendor_orders.columns:
        new_skus_orders = pd.concat([new_skus_orders, vendor_orders[vendor_orders['new_sku']]], ignore_index=True)

# Generate Vendor Order Counts
logging.info("Generating vendor order counts...")
all_processed_orders = pd.concat([label_vendors_orders, non_label_vendors_orders], ignore_index=True)
vendor_order_counts = all_processed_orders.groupby('vendor_name')['order-id'].nunique().reset_index()
vendor_order_counts.columns = ['Vendor', 'Order Count']

# Generate SKU counts, excluding unknown vendors
logging.info("Generating SKU counts, excluding unknown vendors...")
sku_counts_df = all_processed_orders['sku'].value_counts().reset_index()
sku_counts_df.columns = ['SKU', 'Unshipped Orders']

# Safely calculate order counts
label_vendors_count = label_vendors_orders['order-id'].nunique() if not label_vendors_orders.empty else 0
non_label_vendors_count = non_label_vendors_orders['order-id'].nunique() if not non_label_vendors_orders.empty else 0
total_order_count = all_processed_orders['order-id'].nunique() if not all_processed_orders.empty else 0

# Drop unnecessary columns from Label and Non-Label Vendors' reports
columns_to_remove = [
    'order-item-id',
    'payments-date',
    'reporting-date',
    'days-past-promise',
    'buyer-email',
    'buyer-name',
    'payment-method-details',
    'cpf',
    'quantity-shipped',
    'quantity-to-ship',
    'ship-service-level',
    'ship-service-name',
    'ship-address-3',
    'address-type',
    'gift-wrap-type',
    'gift-message-text',
    'payment-method',
    'cod-collectible-amount',
    'already-paid',
    'payment-method-fee',
    'customized-url',
    'customized-page',
    'is-business-order',
    'purchase-order-number',
    'price-designation',
    'is-prime',
    'fulfilled-by',
    'is-premium-order',
    'buyer-company-name',
    'licensee-name',
    'license-number',
    'license-state',
    'license-expiration-date',
    'is-exchange-order',
    'original-order-id',
    'is-transparency',
    'default-ship-from-address-name',
    'default-ship-from-address-field-1',
    'default-ship-from-address-field-2',
    'default-ship-from-address-field-3',
    'default-ship-from-city',
    'default-ship-from-state',
    'default-ship-from-country',
    'default-ship-from-postal-code',
    'is-ispu-order',
    'store-chain-store-id',
    'buyer-requested-cancel-reason',
    'ioss-number',
    'is-shipping-settings-automation-enabled',
    'ssa-carrier',
    'ssa-ship-method',
    'tax-collection-model',
    'tax-collection-responsible-party',
    'verge-of-cancellation',
    'verge-of-lateshipment',
    'signature-confirmation-recommended',
    'history_cost_price_notification',
    'diff_percentage',
    'handling_charges',
    'credit_card_charge',
    'other_charge',
    'min_value',
    'max_value',
    'amazon_commission',
    'ebay_commission',
    'walmart_commission',
    'amazon_profit',
    'ebay_profit',
    'walmart_profit',
    'amazon_price_calculation',
    'ebay_price_calculation',
    'walmart_price_calculation',
    'handling_amazon',
    'handling_ebay',
    'handling_walmart',
    'category',
    'is_map',
    'sales-channel',
    'handling_charge',
    'floor_percentage',
    'ceiling_percentage',
    'promotion_price',
    'promotion_start',
    'promotion_end',
    'brand',
    'price_recal',
    'asin',
    'sku_status',
    'commission',
    'profit',
    'item_weight',
    'item_weight_unit',
    'item_length',
    'item_length_unit',
    'item_height',
    'item_height_unit',
    'item_width',
    'item_width_unit',
    'actual_cost_price',
    'ceiling_price'
]

if not label_vendors_orders.empty:
    label_vendors_orders = label_vendors_orders.drop(columns=columns_to_remove, errors='ignore')
if not non_label_vendors_orders.empty:
    non_label_vendors_orders = non_label_vendors_orders.drop(columns=columns_to_remove, errors='ignore')

# Prepare the final log message
timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
log_message = (
    f"**Timestamp:** {timestamp}\n"
    f"**Total Label Vendors Orders:** {label_vendors_count}\n"
    f"**Total Non-Label Vendors Orders:** {non_label_vendors_count}\n"
    f"**Total Orders:** {total_order_count}\n"
    f"**New SKUs Found:** {len(new_skus_orders)}\n"
    f"**Removed Orders (RET/INV):** {len(removed_orders)}"
)

# Send the final log to Discord
send_to_discord(log_message, title="BETA Unshipped Orders Summary SKU NEW", color=0x00FF00)

# Load bulk buy SKUs from Excel
bulk_buy_skus = set()
try:
    bulk_buy_path = os.path.join(Bulk_buy, 'AMBR-WH Master File 2025.xlsx')  # Update this path
    bulk_buy_df = pd.read_excel(bulk_buy_path)
    # Assuming SKUs are in column C (3rd column, index 2)
    bulk_buy_skus = set(bulk_buy_df.iloc[:, 2].dropna().astype(str))
    logging.info(f"Loaded {len(bulk_buy_skus)} bulk buy SKUs")
except FileNotFoundError:
    logging.warning("Bulk buy Excel not found. Skipping bulk buy check.")
    send_to_discord("Bulk buy Excel not found. Skipping bulk buy check.")
except IndexError:
    logging.error("Bulk buy Excel missing column C")
    send_to_discord("Bulk buy Excel missing column C")
except Exception as e:
    logging.error(f"Error loading bulk buy SKUs: {e}")
    send_to_discord(f"Error loading bulk buy SKUs: {e}")

# Identify bulk buy orders
all_processed_orders = pd.concat([label_vendors_orders, non_label_vendors_orders], ignore_index=True)
bulk_buy_orders = all_processed_orders[all_processed_orders['sku'].isin(bulk_buy_skus)]

if bulk_buy_orders.empty:
    bulk_buy_orders = pd.DataFrame({'Result': ['No SKU Found']})
else:
    bulk_buy_orders = bulk_buy_orders.copy()

# Load returns data
logging.info("Loading returns data...")
returns_path = os.path.join(returns, 'Returns-Dashboard.xlsx')  # Assuming the file is named returns.xlsx
try:
    returns_df = pd.read_excel(returns_path)
    # Assuming Column J is 'SOLD'
    returns_df.columns = returns_df.columns.str.strip().str.lower().str.replace(' ', '_')
    # Filter rows where 'sould' is 'NO'
    returns_df = returns_df[returns_df['SOLD'] == 'NO']
except FileNotFoundError:
    logging.warning("Returns file not found. Skipping returns processing.")
    returns_df = pd.DataFrame()
except Exception as e:
    logging.error(f"Error loading returns data: {e}")
    send_to_discord(f"Error loading returns data: {e}", title="Returns Data Error", color=0xFF0000)
    returns_df = pd.DataFrame()

# Process returns data
if not returns_df.empty:
    # Merge with main orders data to get additional details if needed
    all_processed_orders = pd.concat([label_vendors_orders, non_label_vendors_orders], ignore_index=True)
    merged_returns = pd.merge(
        returns_df,
        all_processed_orders,
        on='sku',
        how='left'
    )
else:
    merged_returns = pd.DataFrame({'Result': ['No Returns Found']})

# Function to fetch SKU details via Eseller API with retries and timeout
def fetch_sku_details(skus, max_retries=3, timeout=30):
    # Eseller API configuration
    host = "https://eseller360.com/api/"
    headers = {
        "Content-Type": "application/json"
    }
    
    # Prepare the request payload
    payload = {
        "sku": skus,
        "fetch_data": [],
        "mp_name": "amazon"
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.post(host, json=payload, headers=headers, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            logging.warning(f"API request timed out (attempt {attempt + 1}/{max_retries})")
        except requests.exceptions.RequestException as e:
            logging.error(f"API request failed: {e} (attempt {attempt + 1}/{max_retries})")
            if attempt == max_retries - 1:
                send_to_discord(f"Failed to fetch SKU details after {max_retries} attempts: {e}", title="SKU Details Error", color=0xFF0000)
                return None  # Return None instead of exiting
            time.sleep(2)  # Wait before retrying
    return None

# Fetch SKU details via API in chunks to prevent large payloads
logging.info("Fetching SKU details via Eseller API...")
unique_skus = all_processed_orders['sku'].unique().tolist()

# Process SKUs in chunks of 50 to avoid large payloads
chunk_size = 10
sku_details = []
for i in range(0, len(unique_skus), chunk_size):
    sku_chunk = unique_skus[i:i + chunk_size]
    logging.info(f"Processing SKU chunk {i//chunk_size + 1}: {len(sku_chunk)} SKUs")
    chunk_response = fetch_sku_details(sku_chunk)
    
    if chunk_response and chunk_response.get('status'):
        chunk_data = chunk_response.get('data', {})
        # Convert the data dictionary to a list of dictionaries
        sku_list = []
        for sku, details in chunk_data.items():
            details['sku'] = sku  # Add the SKU as a field
            sku_list.append(details)
        sku_details.extend(sku_list)
    else:
        logging.error("Failed to fetch details for SKU chunk")
        send_to_discord("Failed to fetch details for SKU chunk", title="SKU Details Error", color=0xFF0000)

# Convert the response to a DataFrame if data was fetched
sku_details_df = pd.DataFrame()
if sku_details:
    try:
        sku_details_df = pd.DataFrame(sku_details)
        
        # Check if 'sku' column exists in the API response
        if 'sku' not in sku_details_df.columns:
            logging.error("API response does not contain 'sku' column")
            send_to_discord("API response does not contain 'sku' column", title="SKU Details Error", color=0xFF0000)
    except Exception as e:
        logging.error(f"Error processing API response: {e}")
        send_to_discord(f"Error processing API response: {e}", title="SKU Details Error", color=0xFF0000)

# Merge SKU details with Label, Non-Label, Unknown Vendors, and bulkbuy orders if data was fetched
if not sku_details_df.empty:
    logging.info("Merging SKU details with Label, Non-Label, Unknown Vendors, and bulkbuy orders...")
    try:
        # Merge Label orders
        if not label_vendors_orders.empty:
            label_vendors_orders = pd.merge(
                label_vendors_orders,
                sku_details_df,
                left_on='sku',
                right_on='sku',
                how='left'
            )
            # Remove columns from merged DataFrame
            label_vendors_orders = label_vendors_orders.drop(columns=columns_to_remove, errors='ignore')
        
        # Merge Non-Label orders
        if not non_label_vendors_orders.empty:
            non_label_vendors_orders = pd.merge(
                non_label_vendors_orders,
                sku_details_df,
                left_on='sku',
                right_on='sku',
                how='left'
            )
            # Remove columns from merged DataFrame
            non_label_vendors_orders = non_label_vendors_orders.drop(columns=columns_to_remove, errors='ignore')
        
        logging.info("SKU details merged successfully.")
    except Exception as e:
        logging.error(f"Error merging SKU details: {e}")
        send_to_discord(f"Error merging SKU details: {e}", title="SKU Details Error", color=0xFF0000)
else:
    logging.warning("No SKU details fetched from API. Skipping merge.")
    send_to_discord("No SKU details fetched from API. Skipping merge.", title="SKU Details Warning", color=0xFFFF00)

# Function to convert UTC time to IST and PDT/PST
def convert_utc_to_timezones(row, date_column):
    utc_tz = pytz.timezone('UTC')
    ist_tz = pytz.timezone('Asia/Kolkata')
    pdt_tz = pytz.timezone('America/Los_Angeles')  # PDT/PST
    
    # Get current time in IST and PDT for reference
    current_ist_time = datetime.now(ist_tz)
    current_pdt_time = datetime.now(pdt_tz)
    
    # Parse the datetime string
    dt_str = row[date_column]
    if pd.isna(dt_str):
        return pd.Series({
            'ist_date': None,
            'ist_time': None,
            'pdt_date': None,
            'pdt_time': None,
            'order_age_ist': None,
            'order_age_pdt': None
        })
    
    # Parse UTC datetime
    dt = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
    dt_utc = dt.replace(tzinfo=utc_tz)
    
    # Convert to IST
    dt_ist = dt_utc.astimezone(ist_tz)
    
    # Convert to PDT/PST
    dt_pdt = dt_utc.astimezone(pdt_tz)
    
    # Calculate time differences
    time_diff_ist = current_ist_time - dt_ist
    hours_diff_ist = round(time_diff_ist.total_seconds() / 3600)
    
    time_diff_pdt = current_pdt_time - dt_pdt
    hours_diff_pdt = round(time_diff_pdt.total_seconds() / 3600)
    
    return pd.Series({
        'ist_date': dt_ist.strftime('%Y-%m-%d'),
        'ist_time': dt_ist.strftime('%H:%M:%S'),
        'pdt_date': dt_pdt.strftime('%Y-%m-%d'),
        'pdt_time': dt_pdt.strftime('%H:%M:%S'),
        'order_age_ist': f"{hours_diff_ist}Hr",
        'order_age_pdt': f"{hours_diff_pdt}Hr"
    })

# Add IST, PDT dates/times, and order ages to Label, Non-Label, Unknown Vendors, and bulkbuy reports
if not label_vendors_orders.empty:
    # Identify the correct column name
    date_columns = [col for col in label_vendors_orders.columns if 'purchase' in col and 'date' in col]
    if date_columns:
        date_column = date_columns[0]
        label_vendors_orders[['ist_date', 'ist_time', 'pdt_date', 'pdt_time', 'order_age_ist', 'order_age_pdt']] = label_vendors_orders.apply(
            convert_utc_to_timezones, axis=1, date_column=date_column
        )
    else:
        logging.warning("No purchase date column found in Label Vendors orders. Skipping time conversion.")
        send_to_discord("No purchase date column found in Label Vendors orders. Skipping time conversion.", 
                       title="Time Conversion Warning", color=0xFFFF00)

if not non_label_vendors_orders.empty:
    # Identify the correct column name
    date_columns = [col for col in non_label_vendors_orders.columns if 'purchase' in col and 'date' in col]
    if date_columns:
        date_column = date_columns[0]
        non_label_vendors_orders[['ist_date', 'ist_time', 'pdt_date', 'pdt_time', 'order_age_ist', 'order_age_pdt']] = non_label_vendors_orders.apply(
            convert_utc_to_timezones, axis=1, date_column=date_column
        )
    else:
        logging.warning("No purchase date column found in Non-Label Vendors orders. Skipping time conversion.")
        send_to_discord("No purchase date column found in Non-Label Vendors orders. Skipping time conversion.", 
                       title="Time Conversion Warning", color=0xFFFF00)

if not unknown_vendors_orders.empty:
    # Identify the correct column name
    date_columns = [col for col in unknown_vendors_orders.columns if 'purchase' in col and 'date' in col]
    if date_columns:
        date_column = date_columns[0]
        unknown_vendors_orders[['ist_date', 'ist_time', 'pdt_date', 'pdt_time', 'order_age_ist', 'order_age_pdt']] = unknown_vendors_orders.apply(
            convert_utc_to_timezones, axis=1, date_column=date_column
        )
    else:
        logging.warning("No purchase date column found in Unknown Vendors orders. Skipping time conversion.")
        send_to_discord("No purchase date column found in Unknown Vendors orders. Skipping time conversion.", 
                       title="Time Conversion Warning", color=0xFFFF00)

# Save output reports
output_file_path = os.path.join(output_path, 'Amazon_Unshipped_Report.xlsx')
logging.info(f"Saving all reports to {output_file_path}...")

try:
    with pd.ExcelWriter(output_file_path, engine='xlsxwriter') as writer:
        label_vendors_orders.to_excel(writer, sheet_name='Label_Vendors', index=False)
        non_label_vendors_orders.to_excel(writer, sheet_name='Non_Label_Vendors', index=False)
        unknown_vendors_orders.to_excel(writer, sheet_name='Unknown_Vendors', index=False)
        sku_counts_df.to_excel(writer, sheet_name='SKU_Counts', index=False)
        vendor_order_counts.to_excel(writer, sheet_name='Vendor_Counts', index=False)
        new_skus_orders.to_excel(writer, sheet_name='New_SKUs', index=False)
        removed_orders.to_excel(writer, sheet_name='Removed_Orders', index=False)
        bulk_buy_orders.to_excel(writer, sheet_name='bulkbuy', index=False)
        if not merged_returns.empty:
            merged_returns.to_excel(writer, sheet_name='Returns', index=False)
        else:
            pd.DataFrame({'Result': ['No Returns Found']}).to_excel(writer, sheet_name='Returns', index=False)
            
    logging.info("Processing complete. All reports including bulkbuy and returns saved.")
except Exception as e:
    logging.error(f"Error saving reports: {e}")
    sys.exit()

time.sleep(3600)
