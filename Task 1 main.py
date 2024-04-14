import gspread

from google.oauth2.service_account import Credentials

import asyncio
import aiohttp
from PIL import Image
from io import BytesIO
import pandas as pd

def write_to_spreadsheet(worksheet, data):
    df = pd.DataFrame(data, columns=['URL', 'Resolution'])

    # Write DataFrame to Google Sheets
    worksheet.update([df.columns.values.tolist()] + df.values.tolist())
async def fetch_urls(worksheet, batch_size):
    total_rows = worksheet.row_count
    urls = []
    column_values = worksheet.col_values(1)
    for i in range(1, total_rows, batch_size):
        start_index = i
        end_index = min(i + batch_size, total_rows)
        batch = column_values[start_index:end_index]
        print(f'{start_index} to {end_index} completed')
        urls.extend(batch)
    return urls

async def fetch_resolution_sizes(session, urls):
    tasks = [fetch_resolution_size(session, url) for url in urls]
    return await asyncio.gather(*tasks)

async def fetch_resolution_size(session, url):
    try:
        async with session.get(url, timeout=60) as response:
            if response.status != 200:
                print(f"Failed to fetch image from {url}. Status code: {response.status}")
                return url, None

            content_type = response.headers.get('Content-Type')
            if content_type and 'image' in content_type:
                image_data = await response.read()

                image = Image.open(BytesIO(image_data))

                resolution = f"{image.width}x{image.height}"
                return url, resolution
            else:
                print(f"Content from {url} is not an image.")
                return url, None
    except aiohttp.ClientError as e:
        print(f"Error fetching image from {url}: {e}")
        return url, None
    except asyncio.TimeoutError:
        print(f"Timeout occurred while fetching image from {url}")
        return url, None

async def main():
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    creds = Credentials.from_service_account_file('credentials.json', scopes=scopes)

    client = gspread.authorize(creds)

    sheet_id = '1QX2IhFyYmGDFMvovw2WFz3wAT4piAZ_8hi5Lzp7LjV0'
    writing_sheet_id = '1YLVs6un2HiNqIQnOhXEyDDXNlO2MebKhC3CfWIwCfBc'
    sheet = client.open_by_key(sheet_id)

    worksheet = sheet.get_worksheet(0)

    batch_size = 5000

    urls = await fetch_urls(worksheet, batch_size)
    async with aiohttp.ClientSession() as session:
        resolution_sizes = []
        for i in range(0, len(urls), batch_size):
            batch_urls = urls[i:i+batch_size]
            print(f'{i} to {i+batch_size}Entered')
            batch_resolution_sizes = await fetch_resolution_sizes(session, batch_urls)
            resolution_sizes.extend(batch_resolution_sizes)
            print(f'{i} to {i+batch_size}Completed')

    resolution_sizes = [(url, size) for url, size in resolution_sizes if size is not None]

    writing_sheet = client.open_by_key(writing_sheet_id)
    writing_work_sheet = writing_sheet.get_worksheet(0)
    write_to_spreadsheet(writing_work_sheet, resolution_sizes)

if __name__ == "__main__":
    asyncio.run(main())
#%%
