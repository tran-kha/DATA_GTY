import os
import shutil
import logging
import time
import json
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import psutil
from collections import OrderedDict

def setup_logging():
    logging.basicConfig(filename='pair_audio_text.log', level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

def find_matching_files(audio_dir, text_dir):
    audio_files = {}
    text_files = {}

    for root, _, files in os.walk(audio_dir):
        for file in files:
            if file.endswith('.mp3'):
                year = os.path.basename(root)
                name = os.path.splitext(file)[0]
                audio_files[(year, name)] = os.path.join(root, file)

    for root, _, files in os.walk(text_dir):
        for file in files:
            if file.endswith('.txt'):
                year = os.path.basename(root)
                name = os.path.splitext(file)[0]
                text_files[(year, name)] = os.path.join(root, file)

    return audio_files, text_files

def load_progress(progress_file):
    if os.path.exists(progress_file):
        try:
            with open(progress_file, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError:
            print(f"Warning: {progress_file} is not a valid JSON file. Starting with empty progress.")
            return {'paired': [], 'last_processed': None}
    return {'paired': [], 'last_processed': None}

def save_progress(progress_file, progress):
    temp_file = progress_file + '.tmp'
    with open(temp_file, 'w') as f:
        json.dump(progress, f)
    os.replace(temp_file, progress_file)

def pair_single_file(args):
    key, audio_path, text_path, output_dir = args
    year, name = key
    
    year_dir = os.path.join(output_dir, year)
    os.makedirs(year_dir, exist_ok=True)
    
    audio_output = os.path.join(year_dir, f"{name}.mp3")
    text_output = os.path.join(year_dir, f"{name}.txt")
    
    try:
        with open(audio_path, 'rb') as src, open(audio_output, 'wb') as dst:
            shutil.copyfileobj(src, dst, length=1024*1024)  # 1MB buffer
        
        with open(text_path, 'rb') as src, open(text_output, 'wb') as dst:
            shutil.copyfileobj(src, dst, length=1024*1024)  # 1MB buffer
        
        audio_filename = os.path.basename(audio_path)
        text_filename = os.path.basename(text_path)
        return f"{audio_filename} paired with {text_filename}"
    except Exception as e:
        logging.error(f"Error pairing files {audio_path} and {text_path}: {str(e)}")
        return None

def get_optimal_workers():
    return min(32, (os.cpu_count() or 1) + 4)

def check_system_resources():
    cpu_percent = psutil.cpu_percent()
    memory_percent = psutil.virtual_memory().percent
    disk_io = psutil.disk_io_counters()
    return (f"CPU Usage: {cpu_percent}%\n"
            f"Memory Usage: {memory_percent}%\n"
            f"Disk Read: {disk_io.read_bytes / 1024 / 1024:.2f} MB\n"
            f"Disk Write: {disk_io.write_bytes / 1024 / 1024:.2f} MB")

def pair_audio_text(audio_files, text_files, output_dir, progress_file):
    progress = load_progress(progress_file)
    paired_count = len(progress['paired'])
    total_files = len(audio_files)
    last_update_time = time.time()
    current_pairs = []
    
    print_lock = threading.Lock()
    performance_stats = {'start_time': time.time(), 'last_check_time': time.time(), 'last_check_count': paired_count}

    def update_progress(future):
        nonlocal paired_count, last_update_time, current_pairs
        try:
            result = future.result()
            with print_lock:
                if result:
                    paired_count += 1
                    current_pairs.append(result)
                
                current_time = time.time()
                if len(current_pairs) >= 10 or (current_time - last_update_time >= 5 and current_pairs):
                    print(f"\nTotal paired: {paired_count}/{total_files}")
                    print("\n".join(current_pairs))
                    print("-" * 50)
                    
                    time_diff = current_time - performance_stats['last_check_time']
                    count_diff = paired_count - performance_stats['last_check_count']
                    speed = count_diff / time_diff if time_diff > 0 else 0
                    overall_speed = paired_count / (current_time - performance_stats['start_time'])
                    print(f"Current speed: {speed:.2f} pairs/second")
                    print(f"Overall speed: {overall_speed:.2f} pairs/second")
                    print(check_system_resources())
                    
                    performance_stats['last_check_time'] = current_time
                    performance_stats['last_check_count'] = paired_count
                    current_pairs = []
                    last_update_time = current_time
        except Exception as e:
            with print_lock:
                logging.error(f"Error processing file: {str(e)}")

    # Sort audio_files by year
    sorted_audio_files = OrderedDict(sorted(audio_files.items(), key=lambda x: x[0][0]))

    with tqdm(total=total_files, initial=paired_count, desc="Pairing files", unit="pair") as pbar:
        with ThreadPoolExecutor(max_workers=get_optimal_workers()) as executor:
            futures = []
            for key, audio_path in sorted_audio_files.items():
                if key in progress['paired']:
                    pbar.update(0)
                    continue
                
                if key in text_files:
                    text_path = text_files[key]
                    future = executor.submit(pair_single_file, (key, audio_path, text_path, output_dir))
                    future.add_done_callback(lambda f: pbar.update(1))
                    future.add_done_callback(update_progress)
                    futures.append(future)
                else:
                    logging.warning(f"Skipped: {os.path.basename(audio_path)} - No matching text file")
                
                if key not in progress['paired']:
                    progress['paired'].append(key)
                    progress['last_processed'] = key
                    save_progress(progress_file, progress)
            
            for future in as_completed(futures):
                pass

    if current_pairs:
        print(f"\nTotal paired: {paired_count}/{total_files}")
        print("\n".join(current_pairs))
        print("-" * 50)

    print("\n")
    total_time = time.time() - performance_stats['start_time']
    print(f"Total time: {total_time:.2f} seconds")
    print(f"Average speed: {paired_count/total_time:.2f} pairs/second")
    return paired_count

def main():
    setup_logging()
    
    audio_dir = "gty_sermons_audio"
    text_dir = "gty_sermons_text"
    output_dir = "gty_sermons_paired"
    progress_file = "pairing_progress.json"

    logging.info("Starting audio-text pairing process")
    print("Starting audio-text pairing process")

    try:
        audio_files, text_files = find_matching_files(audio_dir, text_dir)
        
        logging.info(f"Found {len(audio_files)} audio files and {len(text_files)} text files")
        print(f"Found {len(audio_files)} audio files and {len(text_files)} text files")

        paired_count = pair_audio_text(audio_files, text_files, output_dir, progress_file)

        logging.info(f"Pairing completed. Total paired: {paired_count}/{len(audio_files)}")
        print(f"\nPairing completed. Total paired: {paired_count}/{len(audio_files)}")
        print(f"See 'pair_audio_text.log' for details")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        print(f"An error occurred: {str(e)}")
        print("Check the log file for more details.")

if __name__ == "__main__":
    main()