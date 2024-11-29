from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import gspread
import re
import requests
import time
import json

# Google Sheets API setup
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
]
CREDS = ServiceAccountCredentials.from_json_keyfile_name('config/excel_sheet_key.json', SCOPES)
FILE = gspread.authorize(CREDS)

# Date threshold: 2 weeks ago
TWO_WEEKS_AGO = (datetime.now() - timedelta(weeks=2)).date()


def create_new_sheet(worksheet, sheet_name):
    """Create a new Google Sheets worksheet."""
    try:
        return worksheet.add_worksheet(title=sheet_name, rows="100", cols="2")
    except Exception as e:
        print(f"Error creating new sheet: {e}")
        return None


def extract_dates(text):
    """Extract date in 'YYYY-MM-DD' format from ISO 8601 timestamps."""
    match = re.search(r'\d{4}-\d{2}-\d{2}T', text)
    return match.group()[:-1] if match else None


def store_record_to_google_sheets(sheet, records):
    """Store a record in Google Sheets."""
    try:
        existing_values = sheet.get_all_values()
        start_row = len(existing_values) + 1
        if not existing_values:
            headers = ["Question", "Date", "Link"]
            sheet.insert_row(headers, 1)
            print("Headers added to Google Sheets.")
            start_row += 1

        range_start = f"A{start_row}"
        range_end = f"C{start_row + len(records) - 1}"
        range_to_update = f"{range_start}:{range_end}"

        #Update records in a single API call
        sheet.update(range_to_update, records)
        print(f"Stored {len(records)} records to Google Sheets starting at row {start_row}.")

    except Exception as e:
        print(f"Error storing data in Google Sheets: {e}")


def sheet_exists(worksheet, sheet_name):
    """Check if a worksheet already exists."""
    try:
        return sheet_name in [sheet.title for sheet in worksheet.worksheets()]
    except Exception as e:
        print(f"Error checking sheets: {e}")
        return False


def retry_request(func, retries=3, delay=5):
    """Retry a request multiple times with a delay."""
    for attempt in range(retries):
        try:
            return func()
        except requests.exceptions.ConnectionError as e:
            print(f"Connection error: {e}. Retrying in {delay} seconds...")
            time.sleep(delay)
    raise Exception("Max retries exceeded")


def extract_post_links(description):
    """Extract URLs from a Reddit post description."""
    return re.findall(r'https?://\S+', description)


def fetch_reddit_data(api_url, headers):
    """Fetch Reddit data from API."""
    while True:
        response = requests.get(api_url, headers=headers)
        if response.status_code == 200:
            print("Data scrapped successfully..")
            return response.json()
        elif response.status_code == 202:
            print("Data processing not ready. Waiting for 10 minutes...")
            time.sleep(600)  # Wait for 10 minutes
        else:
            print(f"API request failed. Status code: {response.status_code}, Error: {response.text}")
            return None


def scrape_reddit():
    """Main function to scrape Reddit data and store in Google Sheets."""
    print("Scraping started...")
    new_sheet_name = "Reddit AusPropertyChat Data"
    worksheet = retry_request(lambda: FILE.open('Home Buyer Concerns'))

    sheet = worksheet.worksheet(new_sheet_name) if sheet_exists(worksheet, new_sheet_name) else create_new_sheet(worksheet, new_sheet_name)
    if not sheet:
        print(f"Failed to create or access sheet: {new_sheet_name}")
        return

    # BrightData API setup
    trigger_url = "https://api.brightdata.com/datasets/v3/trigger?dataset_id=gd_lvz8ah06191smkebj4&include_errors=true&type=discover_new&discover_by=subreddit_url"
    snapshot_base_url = "https://api.brightdata.com/datasets/v3/snapshot"
    headers = {"Authorization": "Bearer 80901a9d-4a65-4b5d-8744-d8edc874f5f6", "Content-Type": "application/json"}
    payload = [{"url": "https://www.reddit.com/r/AusPropertyChat/", "sort_by": "New"}]

    # Trigger API
    trigger_response = requests.post(trigger_url, headers=headers, data=json.dumps(payload))
    if trigger_response.status_code != 200:
        print(f"Trigger request failed. Status code: {trigger_response.status_code}, Error: {trigger_response.text}")
        return
    print(f"Snapshot id is created: {trigger_response.json().get("snapshot_id")}")
    snapshot_id = trigger_response.json().get("snapshot_id")
    if not snapshot_id:
        print("Snapshot ID not found in trigger response.")
        return

    reddit_url = f"{snapshot_base_url}/{snapshot_id}?format=json"
    print(f"Scrapping reddit url: {reddit_url}")
    # Fetch Reddit data
    reddit_data = fetch_reddit_data(reddit_url, headers)
    if not reddit_data:
        return

    valid_data = [item for item in reddit_data if 'date_posted' in item]
    sorted_data = sorted(valid_data, key=lambda x: x["date_posted"], reverse=True)
    records_to_store = []
    for post in sorted_data:
        post_date = extract_dates(post.get('date_posted', ''))
        print(f"reddit post date : {post_date}")
        if not post_date:
            continue

        post_date_obj = datetime.strptime(post_date, '%Y-%m-%d').date()
        if post_date_obj < TWO_WEEKS_AGO:
            print(f"Data scrapped successfully of previous Two weeks before Date: {post_date}")
            break

        description = post.get('description', '')
        if description:
            links = extract_post_links(description)
            records_to_store.append([description, post_date, ", ".join(links)])

    if records_to_store:
        store_record_to_google_sheets(sheet, records_to_store)
    return

if __name__ == "__main__":
    scrape_reddit()
