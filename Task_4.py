import pandas as pd
import requests
import os
import psutil
import re
from pathlib import Path
from multiprocessing import Semaphore, Lock
import threading

def download_image(url, semaphore, lock, counter):
    with semaphore:
        try:
            response = requests.get(url, timeout=10)  # Set a timeout for the request
            response.raise_for_status()
        except requests.RequestException as e:
            with lock:
                print(f"Error downloading image {counter['value']}: {e}")
            return

        # Get the image file name from the URL and sanitize it
        file_name = re.sub(r'[\\/:*?"<>|]', '', url[url.rfind('/') + 1:])

        with lock:
            # Increment the counter in a thread-safe way
            counter['value'] += 1
            current_counter = counter['value']

        # Save the image to the 'images' folder on the desktop
        desktop_path = Path.home() / "Desktop"
        images_folder_path = desktop_path / 'images'
        os.makedirs(images_folder_path, exist_ok=True)

        with open(os.path.join(desktop_path, 'images', f'{current_counter}_{file_name}'), 'wb') as f:
            f.write(response.content)

        with lock:
            print(f'Successfully downloaded image {current_counter}')

# Read the Parquet file
parquet_file_path = r"C:\Users\91812\Downloads\links.parquet"
df = pd.read_parquet(parquet_file_path)

# Extract URLs from the 'URL' column
urls = df['URL'].tolist()

# Define a Semaphore to limit concurrent requests
semaphore = Semaphore(5)  # Adjust the value as needed

# Use a lock for thread-safe printing and counter increment
lock = Lock()
counter = {'value': 1}

# Create a list of threads for downloading images
threads = [threading.Thread(target=download_image, args=(url, semaphore, lock, counter)) for url in urls[:10000]]

# Start and join threads
for thread in threads:
    thread.start()
for thread in threads:
    thread.join()

# Monitor the CPU and network usage
cpu_usage = psutil.cpu_percent()
net_usage = psutil.net_io_counters()
print(f'CPU usage: {cpu_usage}%')
print(f'Network usage: {net_usage}')
