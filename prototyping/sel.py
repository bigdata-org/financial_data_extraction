import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# Set up Chrome options
options = Options()
options.add_argument("--headless")  # Run in headless mode for faster scraping

# Set up download directory
download_path = "dumps"
options.add_experimental_option("prefs", {
    "download.default_directory": download_path,  # Set download folder
    "download.prompt_for_download": False,  # No download pop-ups
    "download.directory_upgrade": True,  # Allow upgrades
    "safebrowsing.enabled": True  # Disable Safe Browsing warnings
})

# Initialize WebDriver
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# Function to find and click all .zip links
def download_zip_files(url):
    driver.get(url)  # Navigate to the webpage
    print("Waiting for page to load...")
    
    try:
        # Wait for the page to load (you can adjust the timeout)
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.TAG_NAME, "a"))
        )
        print("Page loaded!")
            # Print the page's HTML content
        page_html = driver.page_source
        print("Page HTML content:")
        print(page_html)  # This will print the entire HTML source of the page
    except:
        print("Page not found within the timeout period.")
        driver.quit()
        return

    # Find all <a> tags on the page
    links = driver.find_elements(By.TAG_NAME, "a")
    print(f"Found {len(links)} links on the page")

    # Filter out the .zip links
    zip_links = [link for link in links if link.get_attribute("href") and link.get_attribute("href").endswith(".zip")]
    print(f"Found {len(zip_links)} .zip links")

    # Click each .zip link to trigger the download
    for link in zip_links:
        href = link.get_attribute("href")
        print(f"Downloading: {href}")
        link.click()  # Click the .zip link to trigger the download
        time.sleep(5)  # Wait for the download to start (adjust as needed)

# Example URL to scrape (replace with the website you're working with)
url = "https://www.sec.gov/data-research/sec-markets-data/financial-statement-data-sets"  # Replace with the actual URL

# Start downloading .zip files
download_zip_files(url)

# Close the WebDriver
driver.quit()
