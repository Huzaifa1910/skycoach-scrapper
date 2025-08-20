"""
Integration module for extracting service options and integrating with the main scraper.
"""

from option_scraper import extract_service_options, save_options_to_csv, scrape_service_options
from selenium import webdriver
from selenium.webdriver.edge.options import Options
from bs4 import BeautifulSoup
import time

def integrate_with_main_scraper():
    """
    Integration function to modify the existing process_nestedlink function 
    to also extract service options
    """
    
    def enhanced_process_nestedlink(nestedlink, row, category, service_id=1):
        """Enhanced version of process_nestedlink that also extracts service options"""
        options = webdriver.EdgeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        
        driver = webdriver.Edge(options=options)
        result = {}
        service_options = []
        
        try:
            if nestedlink.startswith("/"):
                nestedlink = "https://skycoach.gg" + nestedlink
            print(f"Processing: {nestedlink}")
            driver.get(nestedlink)
            time.sleep(3)
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, "html.parser")
            
            # Extract basic service information (existing logic)
            name_tag = soup.find("div", class_="game-header")
            name = None
            if name_tag:
                h1 = name_tag.find("h1")
                if h1:
                    name = h1.get_text(strip=True)
            
            desc = None
            desc_section = soup.find("div", class_="product-info-section__html")
            if desc_section:
                desc = desc_section.get_text(separator="\\n", strip=True)
            
            price = None
            price_span = soup.find("span", class_="payment-summary__price-column-total")
            if price_span:
                price = price_span.get_text(strip=True)
            
            cat = row.get("Category", category)
            
            # Extract service options
            service_options = extract_service_options(page_source, service_id)
            
            # Save options to CSV if any found
            if service_options:
                filename = f"service_options_{service_id}.csv"
                save_options_to_csv(service_options, filename)
                print(f"Extracted {len(service_options)} options for service {service_id}")
            
            result = {
                "Name": name,
                "Description": desc,
                "Price": price,
                "Category": cat,
                "Link": nestedlink,
                "ServiceOptions": len(service_options)  # Count of options extracted
            }
            
        except Exception as e:
            print(f"Error processing {nestedlink}: {str(e)}")
        finally:
            driver.quit()
        
        return result, service_options
    
    return enhanced_process_nestedlink

# Example usage
if __name__ == "__main__":
    enhanced_processor = integrate_with_main_scraper()
    
    # Test with your link
    link = "/wow-classic-hardcore-boost/self-found-powerleveling"
    result, options = enhanced_processor(link, {}, "World of Warcraft Classic Hardcore Boost", service_id=1)
    
    print("\\nService Info:")
    print(f"Name: {result.get('Name')}")
    print(f"Price: {result.get('Price')}")
    print(f"Options Count: {result.get('ServiceOptions')}")
