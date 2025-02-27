import os
import pandas as pd
from multiprocessing import Pool, cpu_count
import logging
import time
import sys
from discord_webhook import DiscordWebhook, DiscordEmbed

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Discord webhook URL
DISCORD_WEBHOOK_URL = 'https://discord.com/api/webhooks/'

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

# Paths for master sheets
old_master_sheet_path = os.path.join(OLD_path, 'OLD_Label_and_NonLabel_Vendors_Updated.xlsx')
new_master_sheet_path = os.path.join(upload_path, '3rd-Party-Orders-Mastersheet.xlsx')

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
    removed_orders = unshipped_orders[unshipped_orders['sku'].str.contains('RET|INV', na=False)].copy()

    # Keep only valid orders
    unshipped_orders = unshipped_orders[~unshipped_orders['sku'].str.contains('RET|INV', na=False)]

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
    'gift-wrap-type',
    'gift-message-text',
    'payment-method',
    'cod-collectible-amount',
    'already-paid',
    'payment-method-fee',
    'customized-url',
    'customized-page',
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
    'signature-confirmation-recommended'
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
send_to_discord(log_message, title="Unshipped Orders Summary FAST", color=0x00FF00)

# Save output reports
output_file_path = os.path.join(output_path, 'Optmized_Unshipped_Report.xlsx')
logging.info(f"Saving all reports to {output_file_path}...")

with pd.ExcelWriter(output_file_path, engine='xlsxwriter') as writer:
    label_vendors_orders.to_excel(writer, sheet_name='Label_Vendors_Orders', index=False)
    non_label_vendors_orders.to_excel(writer, sheet_name='Non_Label_Vendors_Orders', index=False)
    unknown_vendors_orders.to_excel(writer, sheet_name='Unknown_Vendors_Report', index=False)
    sku_counts_df.to_excel(writer, sheet_name='SKU_Counts_Report', index=False)
    vendor_order_counts.to_excel(writer, sheet_name='Vendor_Order_Counts', index=False)
    new_skus_orders.to_excel(writer, sheet_name='New_SKU_Report', index=False)
    removed_orders.to_excel(writer, sheet_name='Removed_Orders', index=False)

logging.info("Processing complete. All reports have been saved.")
time.sleep(3600)
