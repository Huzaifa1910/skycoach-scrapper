import csv
import re
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.edge.options import Options
import time
from datetime import datetime

def parse_price_modifier(price_text):
    """Extract numeric price value from text like '+6,43 €' or 'Free'"""
    if not price_text or price_text.lower() in ['free', 'basic']:
        return 0.00
    
    # Remove currency symbols and extract number
    price_clean = re.sub(r'[^\d,.-]', '', price_text)
    price_clean = price_clean.replace(',', '.')
    
    try:
        return float(price_clean)
    except ValueError:
        return 0.00

def clear_csv_file(filename='service_options.csv'):
    """Clear the CSV file and create a new one with just headers"""
    fieldnames = [
        'option_id', 'service_id', 'parent_option_id', 'option_type', 'option_name',
        'option_label', 'option_value', 'price_modifier', 'min_value', 'max_value',
        'default_value', 'is_required', 'display_order', 'is_active', 'created_at', 'updated_at'
    ]
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
    
    print(f"Cleared CSV file: {filename}")

def get_next_option_id(filename='service_options.csv'):
    """Get the next available option_id from existing CSV file"""
    import os
    if not os.path.exists(filename):
        return 1
    
    try:
        with open(filename, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            max_id = 0
            for row in reader:
                try:
                    option_id = int(row['option_id'])
                    max_id = max(max_id, option_id)
                except (ValueError, KeyError):
                    continue
            return max_id + 1
    except Exception:
        return 1

def get_next_service_id(filename='service_options.csv'):
    """Get the next available service_id from existing CSV file"""
    import os
    if not os.path.exists(filename):
        return 1
    
    try:
        with open(filename, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            max_id = 0
            for row in reader:
                try:
                    service_id = int(row['service_id'])
                    max_id = max(max_id, service_id)
                except (ValueError, KeyError):
                    continue
            return max_id + 1
    except Exception:
        return 1

def extract_service_options(html_content, service_id=None, start_option_id=None):
    """Extract service options from HTML content"""
    soup = BeautifulSoup(html_content, 'html.parser')
    options = []
    
    # Get service ID - auto-increment if not provided
    if service_id is None:
        service_id = get_next_service_id()
    
    # Get starting option ID
    if start_option_id is None:
        option_id_counter = get_next_option_id()
    else:
        option_id_counter = start_option_id
        
    display_order = 1
    
    # Find the main options container
    options_container = soup.find('div', class_='product-detail-calculator__options')
    if not options_container:
        return options
    
    option_groups = options_container.find_all('div', class_='option-group')
    
    for group in option_groups:
        product_option = group.find('div', class_='product-option')
        if not product_option:
            continue
            
        # Get option label from header
        option_head = product_option.find('div', class_='product-option__head')
        option_label = ""
        if option_head:
            label_div = option_head.find('div', class_='product-option__label')
            if label_div:
                option_label = label_div.get_text(strip=True).replace(':', '')
        
        # Check for range/slider input
        range_cluster = product_option.find('div', class_='product-option-cluster-range')
        if range_cluster:
            # Handle range/slider options
            input_containers = range_cluster.find_all('div', class_='input-container')
            
            for idx, container in enumerate(input_containers):
                label_div = container.find('div', class_='label')
                input_tag = container.find('input')
                
                if label_div and input_tag:
                    input_label = label_div.get_text(strip=True)
                    default_val = input_tag.get('value', '')
                    
                    # Extract min/max from range scale if available
                    min_val, max_val = None, None
                    range_container = range_cluster.find('div', class_='range-container')
                    if range_container:
                        scale_items = range_container.find_all('div', class_='range__scale-item')
                        if scale_items:
                            try:
                                min_val = int(scale_items[0].get_text(strip=True))
                                max_val = int(scale_items[-1].get_text(strip=True))
                            except ValueError:
                                pass
                    
                    options.append({
                        'option_id': option_id_counter,
                        'service_id': service_id,
                        'parent_option_id': None,
                        'option_type': 'slider',
                        'option_name': f"{option_label.lower().replace(' ', '_')}_{input_label.lower().replace(' ', '_')}",
                        'option_label': input_label,
                        'option_value': default_val,
                        'price_modifier': 0.00,
                        'min_value': min_val,
                        'max_value': max_val,
                        'default_value': default_val,
                        'is_required': 1,
                        'display_order': display_order,
                        'is_active': 1,
                        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })
                    option_id_counter += 1
                    display_order += 1
        
        # Check for dropdown/select
        select_cluster = product_option.find('div', class_='product-option-cluster-select')
        if select_cluster:
            select_tag = select_cluster.find('select')
            if select_tag:
                parent_id = option_id_counter
                
                # Create parent option for dropdown
                options.append({
                    'option_id': parent_id,
                    'service_id': service_id,
                    'parent_option_id': None,
                    'option_type': 'dropdown',
                    'option_name': option_label.lower().replace(' ', '_').replace(':', ''),
                    'option_label': option_label,
                    'option_value': None,
                    'price_modifier': 0.00,
                    'min_value': None,
                    'max_value': None,
                    'default_value': None,
                    'is_required': 1,
                    'display_order': display_order,
                    'is_active': 1,
                    'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })
                option_id_counter += 1
                display_order += 1
                
                # Add dropdown options
                dropdown_options = select_tag.find_all('option')
                for opt in dropdown_options:
                    option_text = opt.get_text(strip=True)
                    option_val = opt.get('value', '')
                    
                    options.append({
                        'option_id': option_id_counter,
                        'service_id': service_id,
                        'parent_option_id': parent_id,
                        'option_type': 'dropdown',
                        'option_name': f"{option_label.lower().replace(' ', '_').replace(':', '')}_{option_text.lower().replace(' ', '_')}",
                        'option_label': option_text,
                        'option_value': option_val,
                        'price_modifier': 0.00,
                        'min_value': None,
                        'max_value': None,
                        'default_value': option_val if opt.get('selected') else None,
                        'is_required': 0,
                        'display_order': display_order,
                        'is_active': 1,
                        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })
                    option_id_counter += 1
                    display_order += 1
        
        # Check for radio buttons
        radio_cluster = product_option.find('div', class_='product-option-cluster-radios')
        if radio_cluster:
            parent_id = option_id_counter
            
            # Create parent option for radio group
            options.append({
                'option_id': parent_id,
                'service_id': service_id,
                'parent_option_id': None,
                'option_type': 'radio',
                'option_name': option_label.lower().replace(' ', '_').replace(':', ''),
                'option_label': option_label,
                'option_value': None,
                'price_modifier': 0.00,
                'min_value': None,
                'max_value': None,
                'default_value': None,
                'is_required': 1,
                'display_order': display_order,
                'is_active': 1,
                'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
            option_id_counter += 1
            display_order += 1
            
            # Add radio options
            radio_options = radio_cluster.find_all('div', class_='radio-option')
            for radio_opt in radio_options:
                input_tag = radio_opt.find('input', {'type': 'radio'})
                label_span = radio_opt.find('span', class_='radio-check__label')
                price_div = radio_opt.find('div', class_='radio-option__price')
                
                if input_tag and label_span:
                    # Get the label text more carefully - it might have nested elements
                    label_text_parts = []
                    for text_node in label_span.stripped_strings:
                        if text_node.strip() and not text_node.strip().startswith('+') and not text_node.strip().lower() in ['free']:
                            label_text_parts.append(text_node.strip())
                            break  # Take only the first meaningful text
                    
                    option_text = label_text_parts[0] if label_text_parts else label_span.get_text(strip=True).split('\n')[0].strip()
                    option_val = input_tag.get('value', '')
                    is_checked = input_tag.get('checked') is not None
                    
                    price_modifier = 0.00
                    if price_div:
                        price_text = price_div.get_text(strip=True)
                        price_modifier = parse_price_modifier(price_text)
                    
                    options.append({
                        'option_id': option_id_counter,
                        'service_id': service_id,
                        'parent_option_id': parent_id,
                        'option_type': 'radio',
                        'option_name': f"{option_label.lower().replace(' ', '_').replace(':', '')}_{option_text.lower().replace(' ', '_')}",
                        'option_label': option_text,
                        'option_value': option_val,
                        'price_modifier': price_modifier,
                        'min_value': None,
                        'max_value': None,
                        'default_value': option_val if is_checked else None,
                        'is_required': 0,
                        'display_order': display_order,
                        'is_active': 1,
                        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })
                    option_id_counter += 1
                    display_order += 1
        
        # Check for checkboxes
        checkbox_cluster = product_option.find('div', class_='product-option-cluster-checkboxes')
        if checkbox_cluster:
            parent_id = option_id_counter
            
            # Create parent option for checkbox group
            options.append({
                'option_id': parent_id,
                'service_id': service_id,
                'parent_option_id': None,
                'option_type': 'checkbox',
                'option_name': option_label.lower().replace(' ', '_').replace(':', ''),
                'option_label': option_label,
                'option_value': None,
                'price_modifier': 0.00,
                'min_value': None,
                'max_value': None,
                'default_value': None,
                'is_required': 0,
                'display_order': display_order,
                'is_active': 1,
                'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
            option_id_counter += 1
            display_order += 1
            
            # Add checkbox options
            checkbox_options = checkbox_cluster.find_all('div', class_='checkbox-option')
            for checkbox_opt in checkbox_options:
                input_tag = checkbox_opt.find('input', {'type': 'checkbox'})
                label_span = checkbox_opt.find('span', class_='radio-check__label')
                price_div = checkbox_opt.find('div', class_='checkbox-option__price')
                
                if input_tag and label_span:
                    option_text = label_span.get_text(strip=True)
                    option_val = input_tag.get('value', '')
                    is_checked = input_tag.get('checked') is not None
                    
                    price_modifier = 0.00
                    if price_div:
                        price_text = price_div.get_text(strip=True)
                        price_modifier = parse_price_modifier(price_text)
                    
                    options.append({
                        'option_id': option_id_counter,
                        'service_id': service_id,
                        'parent_option_id': parent_id,
                        'option_type': 'checkbox',
                        'option_name': f"{option_label.lower().replace(' ', '_').replace(':', '')}_{option_text.lower().replace(' ', '_').replace('%', 'percent')}",
                        'option_label': option_text,
                        'option_value': option_val,
                        'price_modifier': price_modifier,
                        'min_value': None,
                        'max_value': None,
                        'default_value': option_val if is_checked else None,
                        'is_required': 0,
                        'display_order': display_order,
                        'is_active': 1,
                        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })
                    option_id_counter += 1
                    display_order += 1
    
    return options

def save_options_to_csv(options, filename='service_options.csv', append_mode=True):
    """Save options to CSV file"""
    if not options:
        print("No options to save")
        return
    
    fieldnames = [
        'option_id', 'service_id', 'parent_option_id', 'option_type', 'option_name',
        'option_label', 'option_value', 'price_modifier', 'min_value', 'max_value',
        'default_value', 'is_required', 'display_order', 'is_active', 'created_at', 'updated_at'
    ]
    
    import os
    file_exists = os.path.exists(filename)
    
    if append_mode and file_exists:
        # Append mode: add to existing file without header
        with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writerows(options)
        print(f"Appended {len(options)} options to {filename}")
    else:
        # Write mode: create new file or overwrite with header
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(options)
        print(f"Saved {len(options)} options to {filename}")

def scrape_multiple_services(urls, clear_csv_first=False):
    """
    Scrape multiple service URLs with auto-incrementing service IDs
    
    Args:
        urls: List of URLs to scrape
        clear_csv_first: If True, clears the CSV file before starting
    
    Returns:
        List of tuples: [(service_id, options_count, url), ...]
    """
    if clear_csv_first:
        clear_csv_file()
    
    results = []
    
    for i, url in enumerate(urls):
        print(f"\\n=== Processing URL {i+1}/{len(urls)} ===")
        options = scrape_service_options(url, append_to_csv=True)
        
        if options:
            service_id = options[0]['service_id']
            results.append((service_id, len(options), url))
        else:
            results.append((None, 0, url))
        
        # Small delay between requests
        time.sleep(1)
    
    print(f"\\n=== Summary ===")
    for service_id, count, url in results:
        if service_id:
            print(f"Service {service_id}: {count} options from {url}")
        else:
            print(f"Failed to extract options from {url}")
    
    return results

def scrape_service_options(url, service_id=None, append_to_csv=True):
    """Main function to scrape service options from a URL"""
    options = webdriver.EdgeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    
    driver = webdriver.Edge(options=options)
    
    # Get service ID - auto-increment if not provided
    if service_id is None:
        service_id = get_next_service_id()
    
    try:
        if url.startswith("/"):
            url = "https://skycoach.gg" + url
        
        print(f"Scraping options from: {url} (Service ID: {service_id})")
        driver.get(url)
        time.sleep(3)  # Wait for page to load
        
        page_source = driver.page_source
        options_data = extract_service_options(page_source, service_id)
        
        if options_data:
            save_options_to_csv(options_data, append_mode=append_to_csv)
            print(f"Successfully extracted {len(options_data)} options for service {service_id}")
            return options_data
        else:
            print("No options found on the page")
            return []
            
    except Exception as e:
        print(f"Error scraping options: {str(e)}")
        return []
    finally:
        driver.quit()

# Example usage with your test data
if __name__ == "__main__":
    # Test with the HTML content you provided
    test_html = '''
    <div class="product-detail-calculator__options" data-v-a396af14=""><div class="product-options product-options--desktop product-options--expanded" data-v-7f6697a5="" data-v-a396af14=""><div class="options-container" data-v-7f6697a5=""><div class="option-group" data-v-7f6697a5=""><div class="product-option" data-v-d80641cc="" data-v-7f6697a5=""><!----> <div class="product-option-cluster-range" data-v-d1958270="" data-v-d80641cc=""><div class="input-containers" data-v-d1958270=""><div class="input-container" data-v-e00827a8="" data-v-d1958270=""><div class="text-medium label label--top" data-v-e00827a8="">
    Current Level
  </div> <div class="input-background" data-v-e00827a8=""><!----> <input type="number" value="1" class="input text-medium" data-v-e00827a8=""> <!----> <!----></div> <!----></div> <span class="input-containers__hyphen text-medium" data-v-d1958270="">
        -
      </span> <div class="input-container" data-v-e00827a8="" data-v-d1958270=""><div class="text-medium label label--top" data-v-e00827a8="">
    Desired Level
  </div> <div class="input-background" data-v-e00827a8=""><!----> <input type="number" value="10" class="input text-medium" data-v-e00827a8=""> <!----> <!----></div> <!----></div></div> <div class="range-container" data-v-d1958270=""><div class="range" data-v-b145f792="" data-v-d1958270=""><div class="range__body" data-v-b145f792=""><div data-ignore-click="" class="range__knob" data-v-b145f792="" style="transform: translate(-50%, -50%) translateX(0px);"></div><div data-ignore-click="" class="range__knob" data-v-b145f792="" style="transform: translate(-50%, -50%) translateX(44.1081px);"></div> <div class="range__line" data-v-b145f792="" style="--first-position: 0px; --last-position: 44.1081081081081px;"></div></div> <div class="range__scale" data-v-b145f792=""><div class="range__scale-item text-small transition--color range__scale-item--active" style="--position:0%;" data-v-b145f792="">
      1
    </div><div class="range__scale-item text-small transition--color range__scale-item--active" style="--position:15.315315315315313%;" data-v-b145f792="">
      10
    </div><div class="range__scale-item text-small transition--color" style="--position:32.33233233233233%;" data-v-b145f792="">
      20
    </div><div class="range__scale-item text-small transition--color" style="--position:49.249249249249246%;" data-v-b145f792="">
      30
    </div><div class="range__scale-item text-small transition--color" style="--position:66.16616616616616%;" data-v-b145f792="">
      40
    </div><div class="range__scale-item text-small transition--color" style="--position:83.08308308308308%;" data-v-b145f792="">
      50
    </div><div class="range__scale-item text-small transition--color" style="--position:100%;" data-v-b145f792="">
      60
    </div></div></div></div></div></div> <!----></div><div class="option-group" data-v-7f6697a5=""><div data-has-selection="true" class="product-option" data-v-d80641cc="" data-v-7f6697a5=""><div class="product-option__head" data-v-d80641cc=""><div class="product-option__label text-medium" data-v-d80641cc="">
      Select Your Class:
    </div> <!----></div> <div name="Select Your Class:" class="product-option-cluster-select" data-v-315f180b="" data-v-d80641cc=""><div class="select-container select-container--medium" data-v-5f87ec32="" data-v-315f180b=""><!----> <select aria-hidden="true" value="[object Object]" class="visually-hidden" data-v-5f87ec32=""><option value="10" data-v-5f87ec32="">
      Mage
    </option><option value="5" data-v-5f87ec32="">
      Priest
    </option><option value="8" data-v-5f87ec32="">
      Warlock
    </option><option value="6" data-v-5f87ec32="">
      Druid
    </option><option value="7" data-v-5f87ec32="">
      Paladin
    </option><option value="9" data-v-5f87ec32="">
      Shaman
    </option><option value="11" data-v-5f87ec32="">
      Hunter
    </option><option value="12" data-v-5f87ec32="">
      Rogue
    </option><option value="13" data-v-5f87ec32="">
      Warrior
    </option></select> <div class="select-background" data-v-5f87ec32=""><div tabindex="0" class="select" data-v-5f87ec32=""><span class="text-medium select-label" data-v-5f87ec32="">
        Mage
      </span> <!----> <svg aria-hidden="true" category="arrows" class="ui-icon select-icon" data-v-4d62cf6c="" data-v-5f87ec32=""><use href="/images/icons/sprite.svg?v=17#chevron-down" data-v-4d62cf6c=""></use></svg></div> <!----></div> <!----></div></div></div> <!----></div><div class="option-group" data-v-7f6697a5=""><div data-has-selection="true" class="product-option" data-v-d80641cc="" data-v-7f6697a5=""><div class="product-option__head" data-v-d80641cc=""><div class="product-option__label text-medium" data-v-d80641cc="">
      Insurance Options:
    </div> <!----></div> <div class="product-option-cluster-radios" data-v-c1cb46a2="" data-v-d80641cc=""><div class="radios-group" data-v-c1cb46a2=""><div class="radio-option" data-v-c1cb46a2=""><label class="radio-check radio-check--with-label radio-check--active radio-check--violet" data-v-6efbd682="" data-v-c1cb46a2=""><input id="22" type="radio" name="Insurance Options:" hidden="hidden" value="22" checked="checked" data-v-6efbd682=""> <span class="radio-check__shape radio-check__shape--dot radio-check__shape--dot-active" data-v-6efbd682=""><span class="radio-check__dot" data-v-6efbd682=""></span></span> <span class="radio-check__label text-small" data-v-6efbd682="">
        Basic

        <div class="radio-option__info" data-v-6efbd682="" data-v-c1cb46a2=""><div class="radio-option__price text-small" data-v-6efbd682="" data-v-c1cb46a2="">
            Free
          </div> <!----></div></span></label></div><div class="radio-option" data-v-c1cb46a2=""><label class="radio-check radio-check--with-label radio-check--violet" data-v-6efbd682="" data-v-c1cb46a2=""><input id="23" type="radio" name="Insurance Options:" hidden="hidden" value="23" checked="checked" data-v-6efbd682=""> <span class="radio-check__shape radio-check__shape--dot" data-v-6efbd682=""><span class="radio-check__dot" data-v-6efbd682=""></span></span> <span class="radio-check__label text-small" data-v-6efbd682="">
        Extended

        <div class="radio-option__info" data-v-6efbd682="" data-v-c1cb46a2=""><div class="radio-option__price text-small" data-v-6efbd682="" data-v-c1cb46a2="">
            +6,43&nbsp;€
          </div> <!----></div></span></label></div><div class="radio-option" data-v-c1cb46a2=""><label class="radio-check radio-check--with-label radio-check--violet" data-v-6efbd682="" data-v-c1cb46a2=""><input id="24" type="radio" name="Insurance Options:" hidden="hidden" value="24" checked="checked" data-v-6efbd682=""> <span class="radio-check__shape radio-check__shape--dot" data-v-6efbd682=""><span class="radio-check__dot" data-v-6efbd682=""></span></span> <span class="radio-check__label text-small" data-v-6efbd682="">
        Premium

        <div class="radio-option__info" data-v-6efbd682="" data-v-c1cb46a2=""><div class="radio-option__price text-small" data-v-6efbd682="" data-v-c1cb46a2="">
            +12,87&nbsp;€
          </div> <!----></div></span></label></div></div></div></div> <!----></div><div class="option-group" data-v-7f6697a5=""><div class="product-option" data-v-d80641cc="" data-v-7f6697a5=""><div class="product-option__head" data-v-d80641cc=""><div class="product-option__label text-medium" data-v-d80641cc="">
      Choose Extras
    </div> <!----></div> <div name="Choose Extras" class="product-option-cluster-checkboxes" data-v-2778a70f="" data-v-d80641cc=""><div class="checkboxes-group" data-v-2778a70f=""><div class="checkbox-option" data-v-2778a70f=""><label class="radio-check radio-check--with-label radio-check--violet" data-v-6efbd682="" data-v-2778a70f=""><input id="1" type="checkbox" name="" hidden="hidden" value="1" data-v-6efbd682=""> <span class="radio-check__shape radio-check__shape--check radio-check__shape--check-hidden" data-v-6efbd682=""><svg aria-hidden="true" class="ui-icon radio-check__checkmark" data-v-4d62cf6c="" data-v-6efbd682=""><use href="/images/icons/sprite.svg?v=17#tick" data-v-4d62cf6c=""></use></svg></span> <span class="radio-check__label text-small" data-v-6efbd682="">
        Streaming
      </span></label> <div class="checkbox-option__info" data-v-2778a70f=""><div class="checkbox-option__price text-small" data-v-2778a70f="">
          +8,99&nbsp;€
        </div> <!----></div></div><div class="checkbox-option" data-v-2778a70f=""><label class="radio-check radio-check--with-label radio-check--violet" data-v-6efbd682="" data-v-2778a70f=""><input id="25" type="checkbox" name="" hidden="hidden" value="25" data-v-6efbd682=""> <span class="radio-check__shape radio-check__shape--check radio-check__shape--check-hidden" data-v-6efbd682=""><svg aria-hidden="true" class="ui-icon radio-check__checkmark" data-v-4d62cf6c="" data-v-6efbd682=""><use href="/images/icons/sprite.svg?v=17#tick" data-v-4d62cf6c=""></use></svg></span> <span class="radio-check__label text-small" data-v-6efbd682="">
        60% Riding Skill
      </span></label> <div class="checkbox-option__info" data-v-2778a70f=""><div class="checkbox-option__price text-small" data-v-2778a70f="">
          +65,98&nbsp;€
        </div> <!----></div></div><div class="checkbox-option" data-v-2778a70f=""><label class="radio-check radio-check--with-label radio-check--violet" data-v-6efbd682="" data-v-2778a70f=""><input id="26" type="checkbox" name="" hidden="hidden" value="26" data-v-6efbd682=""> <span class="radio-check__shape radio-check__shape--check radio-check__shape--check-hidden" data-v-6efbd682=""><svg aria-hidden="true" class="ui-icon radio-check__checkmark" data-v-4d62cf6c="" data-v-6efbd682=""><use href="/images/icons/sprite.svg?v=17#tick" data-v-4d62cf6c=""></use></svg></span> <span class="radio-check__label text-small" data-v-6efbd682="">
        100% Riding Skill
      </span></label> <div class="checkbox-option__info" data-v-2778a70f=""><div class="checkbox-option__price text-small" data-v-2778a70f="">
          +519,99&nbsp;€
        </div> <!----></div></div></div></div></div> <!----></div></div> <!----></div></div>
    '''
    
    # Test the extraction function
    print("=== Testing auto-incrementing service IDs ===")
    
    # First service - will get next available service_id automatically
    options = extract_service_options(test_html)
    if options:
        service_id_1 = options[0]['service_id']
        print(f"\\nFirst service got ID: {service_id_1}")
        # First time: create new file with header
        save_options_to_csv(options, append_mode=False)
        print("\\nExtracted options:")
        for opt in options[:5]:  # Show first 5 options
            print(f"- {opt['option_type']}: {opt['option_label']} (Service: {opt['service_id']}, Price: {opt['price_modifier']})")
    
    # Second service - will automatically get the next service_id
    print("\\n--- Testing append mode with auto-increment ---")
    options_service2 = extract_service_options(test_html)
    if options_service2:
        service_id_2 = options_service2[0]['service_id']
        print(f"Second service got ID: {service_id_2}")
        # Append to existing file
        save_options_to_csv(options_service2, append_mode=True)
        print(f"Appended {len(options_service2)} options for service {service_id_2}")
    
    # Third service - will get the next service_id again
    options_service3 = extract_service_options(test_html)
    if options_service3:
        service_id_3 = options_service3[0]['service_id']
        print(f"Third service got ID: {service_id_3}")
        save_options_to_csv(options_service3, append_mode=True)
        print(f"Appended {len(options_service3)} options for service {service_id_3}")
    
    print(f"\\nDemo complete! Service IDs auto-incremented: {service_id_1} -> {service_id_2} -> {service_id_3}")
    
    # Uncomment the line below to scrape from a live URL with auto service_id
    # scrape_service_options("/wow-classic-hardcore-boost/self-found-powerleveling", append_to_csv=True)
