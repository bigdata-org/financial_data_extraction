# from bs4 import BeautifulSoup
# import requests

# def scrape_zip_links(url):
#     headers = {
#         'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
#     }
#     response = requests.get(url, headers=headers)
#     soup = BeautifulSoup(response.content, 'html.parser')
    
#     # Find all links that contain ZIP files
#     zip_links = []
#     for link in soup.find_all('a'):
#         href = link.get('href', '')
#         # if any(quarter in href.lower() for quarter in ['2024 q4', '2024 q3', '2024 q2', 
#         #        '2024 q1', '2023 q4', '2023 q3', '2023 q2']):
#         zip_links.append(href)
    
#     return zip_links

# print(scrape_zip_links("https://www.sec.gov/data-research/sec-markets-data/financial-statement-data-sets"))


from bs4 import BeautifulSoup
import requests

def scrape_sec_zips(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
    }
    
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Find all links with ZIP files
    zip_links = []
    for link in soup.find_all('a'):
        href = link.get('href', '')
        text = link.text.strip()
        # Look for quarterly patterns in either href or text
       
        zip_links.append(href)
    
    return zip_links

# Example usage
url = "https://www.sec.gov/data-research/sec-markets-data/financial-statement-data-sets"
zip_files = scrape_sec_zips(url)

print(zip_files)