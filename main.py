import argparse
import requests
from bs4 import BeautifulSoup
import os
import json
from concurrent.futures import ThreadPoolExecutor
import time
import threading
from datetime import datetime
import logging
from tqdm import tqdm

class ProgressTracker:
    def __init__(self, progress_file, total_urls):
        self.successful_downloads = 0
        self.current_url = ""
        self.progress_file = progress_file
        self.progress = {}
        self.year_counts = {}
        self.lock = threading.Lock()
        self.load_progress()
        self.start_time = datetime.now()
        self.total_urls = total_urls
        self.pbar = tqdm(total=total_urls, unit="file")

    def increment_downloads(self, year, number):
        with self.lock:
            self.successful_downloads += 1
            if year not in self.progress:
                self.progress[year] = []
            self.progress[year].append(number)
            self.year_counts[year] = self.year_counts.get(year, 0) + 1
            self.save_progress()
            self.pbar.update(1)

    def update_current_url(self, url):
        with self.lock:
            self.current_url = url
            self.pbar.set_description(f"Processing: {url}")

    def get_stats(self):
        with self.lock:
            return self.successful_downloads, self.current_url, self.year_counts

    def load_progress(self):
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r') as f:
                    data = json.load(f)
                    self.successful_downloads = data.get('successful_downloads', 0)
                    self.progress = data.get('files_success', {})
                    self.year_counts = {year: len(items) for year, items in self.progress.items()}
            except json.JSONDecodeError:
                logging.error(f"Lỗi: File {self.progress_file} không phải là JSON hợp lệ. Tạo file progress mới.")
                self.reset_progress()
            except Exception as e:
                logging.error(f"Lỗi khi đọc file {self.progress_file}: {str(e)}. Tạo file progress mới.")
                self.reset_progress()
        else:
            logging.info(f"File {self.progress_file} không tồn tại. Tạo file progress mới.")
            self.reset_progress()

    def reset_progress(self):
        self.successful_downloads = 0
        self.progress = {}
        self.year_counts = {}

    def save_progress(self):
        with open(self.progress_file, 'w') as f:
            json.dump({
                'successful_downloads': self.successful_downloads,
                'files_success': self.progress
            }, f, indent=2)

    def get_session_time(self):
        return str(datetime.now() - self.start_time).split('.')[0]

    def close(self):
        self.pbar.close()

def extract_text_from_gty(url, filename, tracker, year, number):
    try:
        tracker.update_current_url(url)
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        transcript_content = soup.find('section', class_='transcript-content gty-writing-content')

        if transcript_content:
            text = transcript_content.text.strip()
            if text:
                os.makedirs(os.path.dirname(filename), exist_ok=True)
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(text)
                tracker.increment_downloads(year, number)
                logging.info(f"Đã lưu thành công: {url}")
                return True
        logging.warning(f"Không tìm thấy nội dung cho URL: {url}")
        return False
    except requests.RequestException as e:
        logging.error(f"Lỗi khi truy cập URL {url}: {str(e)}")
        return False
    except Exception as e:
        logging.error(f"Lỗi không xác định khi xử lý URL {url}: {str(e)}")
        return False

def process_url(url_info, output_dir, tracker):
    url, year, number = url_info
    if year in tracker.progress and number in tracker.progress[year]:
        logging.info(f"Bỏ qua URL đã xử lý: {url}")
        return False, None

    year_dir = os.path.join(output_dir, year)
    filename = os.path.join(year_dir, f"{number}.txt")
    success = extract_text_from_gty(url, filename, tracker, year, number)
    if not success and os.path.exists(filename):
        os.remove(filename)
    time.sleep(0.5)  # Nghỉ 1 giây sau mỗi lần tải
    return success, filename

def print_progress(tracker, total_urls, stop_event):
    while not stop_event.is_set():
        downloads, current_url, year_counts = tracker.get_stats()
        session_time = tracker.get_session_time()
        elapsed_time = time.time() - tracker.start_time.timestamp()
        speed = downloads / elapsed_time if elapsed_time > 0 else 0
        estimated_time = (total_urls - downloads) / speed if speed > 0 else 0
        
        print(f"\rTiến độ: {downloads}/{total_urls} | Tốc độ: {speed:.2f} file/s | Ước tính còn lại: {estimated_time:.0f}s", end="")
        time.sleep(1)  # Cập nhật mỗi giây

def count_files_in_directories(base_dir):
    total_files = 0
    dir_counts = {}

    for root, dirs, files in os.walk(base_dir):
        dir_name = os.path.basename(root)
        if dir_name.isdigit():  # Chỉ đếm trong thư mục năm
            file_count = len(files)
            dir_counts[dir_name] = file_count
            total_files += file_count

    return dir_counts, total_files

def generate_urls_from_json(json_file):
    with open(json_file, 'r') as f:
        data = json.load(f)
    
    urls = []
    base_url = "https://www.gty.org/library/sermons-library/{}"
    
    for year, year_data in data.items():
        for item in year_data['items']:
            url = base_url.format(item)
            urls.append((url, year, item))
    
    return urls



def generate_urls_from_json(json_file, start_year=None, end_year=None, specific_url=None):
    with open(json_file, 'r') as f:
        data = json.load(f)
    
    urls = []
    base_url = "https://www.gty.org/library/sermons-library/{}"
    
    for year, year_data in data.items():
        if start_year and int(year) < start_year:
            continue
        if end_year and int(year) > end_year:
            break
        
        for item in year_data['items']:
            url = base_url.format(item)
            if specific_url and url != specific_url:
                continue
            urls.append((url, year, item))
    
    return urls
def main():
    parser = argparse.ArgumentParser(description="Scrape sermons from GTY.org")
    parser.add_argument("--url", help="Specific URL to scrape")
    parser.add_argument("--year", type=int, help="Specific year to scrape")
    parser.add_argument("--start-year", type=int, help="Start year for scraping range")
    parser.add_argument("--end-year", type=int, help="End year for scraping range")
    args = parser.parse_args()
    log_file = "gty_scraper.log"
    logging.basicConfig(filename=log_file, level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("Bắt đầu chương trình...")
    output_dir = "gty_sermons"
    os.makedirs(output_dir, exist_ok=True)
    progress_file = "progress.json"
    json_file = "combined_gty_sermons.json"
    if args.url:
        urls = generate_urls_from_json(json_file, specific_url=args.url)
    elif args.year:
        urls = generate_urls_from_json(json_file, start_year=args.year, end_year=args.year)
    elif args.start_year and args.end_year:
        urls = generate_urls_from_json(json_file, start_year=args.start_year, end_year=args.end_year)
    else:
        urls = generate_urls_from_json(json_file)
    total_urls = len(urls)
    tracker = ProgressTracker(progress_file, total_urls)
    logging.info(f"Đã tạo {total_urls} URLs")
    stop_event = threading.Event()
    progress_thread = threading.Thread(target=print_progress, args=(tracker, total_urls, stop_event))
    progress_thread.start()
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            results = list(executor.map(lambda url_info: process_url(url_info, output_dir, tracker), urls))
    finally:
        stop_event.set()
        progress_thread.join()
        tracker.close()
    successful_downloads, _, year_counts = tracker.get_stats()
    print("\n")  # Xuống dòng sau khi hoàn thành
    logging.info(f"Tổng số URL đã xử lý: {total_urls}")
    logging.info(f"Số file text đã được tải thành công: {successful_downloads}")
    logging.info(f"Tổng thời gian chạy: {tracker.get_session_time()}")
    logging.info("Số lượng item đã lưu cho mỗi năm:")
    for year, count in sorted(year_counts.items()):
        logging.info(f"{year}: {count}")
    dir_counts, total_files = count_files_in_directories(output_dir)
    print("\nSố lượng bài giảng trong mỗi năm:")
    for year, count in sorted(dir_counts.items()):
        print(f"{year}: {count}")
    print(f"\nTổng số file txt trong thư mục mẹ: {total_files}")
    print(f"\nChương trình đã kết thúc. Xem file log {log_file} để biết chi tiết.")
if __name__ == "__main__":
    main()