import pytest
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import time
import logging

logger = logging.getLogger(__name__)

def test_discover_total_pages():
    """Test that we can correctly identify the total number of pages."""
    # Set up Chrome WebDriver
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    try:
        base_url = "https://www.databricks.com/dataaisummit/agenda"
        current_page = 1
        total_pages = 0
        
        # Process pages until no more sessions are found
        while True:
            # Construct the URL with the page parameter
            page_url = f"{base_url}?page={current_page}"
            logger.info(f"Checking page {current_page}")
            
            # Navigate to the page
            driver.get(page_url)
            time.sleep(2)  # Wait for page to load
            
            # Find session links
            session_links = driver.find_elements(
                By.CSS_SELECTOR,
                'a[href*="/session/"]'
            )
            
            # If no session links found, we've reached the end
            if not session_links:
                logger.info(f"No more sessions found on page {current_page}")
                break
            
            # Filter out "SEE DETAILS" links
            page_urls = set()
            for link in session_links:
                if "SEE DETAILS" not in link.text:
                    href = link.get_attribute("href")
                    if href:
                        page_urls.add(href)
            
            # If no new URLs found on this page, we've reached the end
            if not page_urls:
                logger.info(f"No new session URLs found on page {current_page}")
                break
            
            total_pages = current_page
            current_page += 1
        
        # Verify we found at least one page
        assert total_pages > 0, "Should have found at least one page"
        
        # Log the total number of pages found
        logger.info(f"Found {total_pages} total pages")
        
        # Optional: You can add more specific assertions here
        # For example, if you know there should be at least X pages:
        assert total_pages >= 34, f"Expected at least 34 pages, but found {total_pages}"
        
    finally:
        driver.quit()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_discover_total_pages() 