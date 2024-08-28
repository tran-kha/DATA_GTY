import argparse
import requests
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

def download_audio(url, filename, tracker, year, number):
    try:
        tracker.update_current_url(url)
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        
        tracker.increment_downloads(year, number)
        logging.info(f"Đã tải xuống thành công: {url}")
        return True
    except requests.RequestException as e:
        logging.error(f"Lỗi khi tải xuống URL {url}: {str(e)}")
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
    filename = os.path.join(year_dir, f"{number}.mp3")
    success = download_audio(url, filename, tracker, year, number)
    if not success and os.path.exists(filename):
        os.remove(filename)
    time.sleep(0.5)  # Nghỉ 0.5 giây sau mỗi lần tải
    return success, filename

def generate_urls_from_json(json_file, start_year=None, end_year=None, specific_url=None):
    with open(json_file, 'r') as f:
        data = json.load(f)
    
    urls = []
    base_url = "https://cdn.gty.org/sermons/High/{}.mp3"
    
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
    parser = argparse.ArgumentParser(description="Download audio sermons from GTY.org")
    parser.add_argument("--url", help="Specific URL to download")
    parser.add_argument("--year", type=int, help="Specific year to download")
    parser.add_argument("--start-year", type=int, help="Start year for download range")
    parser.add_argument("--end-year", type=int, help="End year for download range")
    args = parser.parse_args()

    log_file = "gty_audio_downloader.log"
    logging.basicConfig(filename=log_file, level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("Bắt đầu chương trình...")

    output_dir = "gty_audio_sermons"
    os.makedirs(output_dir, exist_ok=True)
    progress_file = "audio_progress.json"
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

    try:
        with ThreadPoolExecutor(max_workers=5) as executor:
            results = list(executor.map(lambda url_info: process_url(url_info, output_dir, tracker), urls))
    finally:
        tracker.close()

    successful_downloads, _, year_counts = tracker.get_stats()
    print("\n")  # Xuống dòng sau khi hoàn thành
    logging.info(f"Tổng số URL đã xử lý: {total_urls}")
    logging.info(f"Số file audio đã được tải thành công: {successful_downloads}")
    logging.info(f"Tổng thời gian chạy: {tracker.get_session_time()}")
    logging.info("Số lượng file đã tải cho mỗi năm:")
    for year, count in sorted(year_counts.items()):
        logging.info(f"{year}: {count}")

    print(f"\nChương trình đã kết thúc. Xem file log {log_file} để biết chi tiết.")

if __name__ == "__main__":
    main()