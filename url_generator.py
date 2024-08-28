# url_generator.py

import json

def generate_urls():
    file_path = 'combined_gty_sermons.json'
    base_url = "https://www.gty.org/library/sermons-library/{}"
    urls = []

    # Đọc file JSON
    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    # Lặp qua từng năm và các mục trong năm đó để tạo URL
    for year, details in data.items():
        items = details.get("items", [])
        for item in items:
            url = base_url.format(item)
            urls.append((url, year, item))
    
    return urls