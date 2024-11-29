from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
from fake_useragent import UserAgent

import gspread
import re
import asyncio
import random
import requests
import time

# Google Sheets API setup
scopes = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
]
creds = ServiceAccountCredentials.from_json_keyfile_name('config/excel_sheet_key.json', scopes)
file = gspread.authorize(creds)

# Set the date threshold to 2 weeks ago from today
two_weeks_ago = (datetime.now() - timedelta(weeks=2)).date()

# Function to create a new sheet
def create_new_sheet(worksheet, sheet_name):
    try:
        new_sheet = worksheet.add_worksheet(title=sheet_name, rows="100", cols="2")
        print(f"New sheet '{sheet_name}' created.")
        return new_sheet
    except Exception as e:
        print(f"Error creating new sheet: {e}")
        return None

def extract_dates(text: str):
    """
    Extracts date as a string from a timestamp in ISO 8601 format.

    Args:
        text (str): Timestamp string in ISO 8601 format.

    Returns:
        str: Date in 'YYYY-MM-DD' format, or None if no date is found.
    """

    # Define the regex pattern for matching ISO 8601 timestamp strings
    date_pattern = r'\d{4}-\d{2}-\d{2}T'
    match = re.search(date_pattern, text)
    return match.group()[:-1] if match else None

def clean_text(text):
    """
    Cleans the text by removing unnecessary whitespace.
    """
    return " ".join(text.split())

# Function to store each record in Google Sheets immediately
def store_record_to_google_sheets(sheet, question, date, link):
    try:
        links_str = ", ".join(link)
        data_to_insert = [question, date, links_str]

        existing_values = sheet.get_all_values()
        if len(existing_values[0]) == 0:
            headers = ["Question", "Date", "Link"]
            sheet.insert_row(headers, 1)
            print("Headers added to Google Sheets.")

        next_row = len(existing_values) + 1

        sheet.insert_row(data_to_insert, next_row)
        print(f'Successfully stored record at row {next_row}: {question}| with date:{date}| Link:{links_str}')

    except Exception as e:
        print(f'Error storing data in Google Sheets: {e}')

def sheet_exists(worksheet, sheet_name):
    try:
        # Retrieve all sheet names
        existing_sheets = [sheet.title for sheet in worksheet.worksheets()]
        return sheet_name in existing_sheets
    except Exception as e:
        print(f"Error checking for existing sheets: {e}")
        return False

def retry_request(func, retries=3, delay=5):
    for attempt in range(retries):
        try:
            return func()
        except requests.exceptions.ConnectionError as e:
            print(f"Connection error: {e}. Retrying in {delay} seconds...")
            time.sleep(delay)
    raise Exception("Max retries exceeded")


async def scrape_reddit():
    print("Scrapping started..")
    async with async_playwright() as p:
        # Launch a headless browser
        browser = await p.chromium.launch(
            headless=False,
            args=[
                '--start-maximized',
                '--disable-gpu',
                '--no-sandbox',
                '--disable-software-rasterizer',
                '--enable-features=NetworkService,NetworkServiceInProcess',
                '--disable-blink-features=AutomationControlled'
            ]
        )
        # Generate a random user-agent using fake_useragent
        ua = UserAgent()
        user_agent = ua.random

        page = await browser.new_page(user_agent=user_agent)
        new_sheet_name = "Reddit AusPropertyChat Data"

        worksheet = retry_request(lambda: file.open('Home Buyer Concerns'))

        try:
            await page.goto("https://www.reddit.com/r/AusPropertyChat/", timeout=30000)
        except PlaywrightTimeoutError:
            print("Page load timed out.")
            await browser.close()
            return

        if not sheet_exists(worksheet, new_sheet_name):
            sheet = create_new_sheet(worksheet, new_sheet_name)
        else:
            sheet = worksheet.worksheet(new_sheet_name)

        # Data storage and scroll tracking
        data = []  # Accumulate all post data here
        tw_found = False
        await page.screenshot(path="screenshot.png", full_page=True)
        previous_height = await page.evaluate('document.body.scrollHeight')

        # Scroll and capture posts
        while True:
            await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
            print("Scrolling the page towards bottom")
            await page.wait_for_timeout(random.randint(5000, 8000))  # Wait for new content to load

            # Capture current posts
            posts = await page.query_selector_all('article.w-full.m-0')
            for post in posts:
                post_date = ""
                post_text = ""
                content_div = await post.query_selector('div[data-post-click-location="text-body"]')
                if content_div:
                    post_text = clean_text(await content_div.inner_text())
                    link_tags = await content_div.query_selector_all('a')
                    links = [
                        (await link.get_attribute('href')).replace('/r/ausproperty/',
                                                                   'https://www.reddit.com/r/AusPropertyChat/')
                        for link in link_tags if await link.get_attribute('href')
                    ]

                date_obj = await post.query_selector('time')
                date_str = await date_obj.get_attribute('datetime') if date_obj else None
                post_date = extract_dates(date_str) if date_str else None
                # Check if both title and link exist
                if post_text and post_date:
                    post_date_obj = datetime.strptime(post_date, '%Y-%m-%d').date()
                    print(f'extracted date is {post_date_obj}')
                    # if post_date_obj >= two_weeks_ago:
                    post_data = {
                        'question_post': post_text,
                        'date': post_date,
                        'link': links
                    }
                    # Only add unique posts based on URL
                    if post_data not in data:
                        data.append(post_data)  # Accumulate data instead of storing it immediately

            # Break if two weeks threshold reached or end of page is reached
            if tw_found:
                break

            new_height = await page.evaluate('document.body.scrollHeight')
            if new_height == previous_height:
                print("Reached end of page")
                break
            previous_height = new_height

        # Now store all data at once
        if data:
            print("Storing all data to Google Sheets at once.")
            # Prepare data for batch insert into Google Sheets
            data_to_insert = []
            for post_data in data:
                # Convert data into the required format (Question, Date, Links)
                links_str = ", ".join(post_data['link'])
                data_to_insert.append([post_data['question_post'], post_data['date'], links_str])

            # Store data in Google Sheets
            try:
                existing_values = sheet.get_all_values()
                if len(existing_values[0]) == 0:
                    headers = ["Question", "Date", "Link"]
                    sheet.insert_row(headers, 1)
                    print("Headers added to Google Sheets.")

                # Insert all records at once
                sheet.insert_rows(data_to_insert, row=2)
                print(f"Successfully stored {len(data_to_insert)} records to Google Sheets.")
            except Exception as e:
                print(f'Error storing data in Google Sheets: {e}')

        # Close the browser
        await browser.close()
        print("Scraping process completed successfully.")


# Run the scraping function
asyncio.run(scrape_reddit())
