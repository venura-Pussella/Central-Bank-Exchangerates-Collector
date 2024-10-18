import requests
from bs4 import BeautifulSoup
from src.configuration.configuration import webpage_url

def fetch_webpage(url):
    response = requests.get(url)
    response.raise_for_status()  # Ensure the request was successful
    return BeautifulSoup(response.content, 'html.parser')

# Function to find the Excel file link
def find_excel_link(soup, css_selector):
    link_element = soup.select_one(css_selector)
    if link_element:
        excel_url = link_element.get('href')
        # Handle relative URLs
        if not excel_url.startswith('http'):
            excel_url = requests.compat.urljoin(webpage_url, excel_url)
        return excel_url
    else:
        raise ValueError("Excel file link not found.")