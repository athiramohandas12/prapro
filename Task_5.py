import os
import requests
import pandas as pd
import pyarrow.parquet as pq

class Downloader:
    def __init__(self, pq_file: str):
        self.pq_file = pq_file
        self.image_urls = self.extract_image_urls()

    def extract_image_urls(self):
        # Use pyarrow to read parquet file and extract image URLs
        table = pq.read_table(self.pq_file)
        
        # Inspect the schema to find the correct column name
        column_names = table.schema.names
        print("Column names in the Parquet file:", column_names)
        
        # Adjust the column name based on the actual schema
        if 'URL' in column_names:
            urls_column = table.column('URL')
            return list(urls_column.to_pylist())
        else:
            raise ValueError("Column 'URL' not found in the Parquet file schema.")

    def clean_filename(self, filename: str):
        # Replace invalid characters in the filename
        invalid_chars = ['\\', '/', ':', '*', '?', '"', '<', '>', '|']
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        return filename

    def download_image(self, url: str, destination: str):
        response = requests.get(str(url))  # Convert StringScalar to a regular string
        if response.status_code == 200:
            # Extract the filename from the URL and clean it
            filename = os.path.basename(str(url))  # Convert StringScalar to a regular string
            filename = self.clean_filename(filename)
            
            # Combine the cleaned filename with the destination path
            local_path = os.path.join(destination, filename)
            
            with open(local_path, 'wb') as file:
                file.write(response.content)
                print(f"Downloaded image: {local_path}")

    def __getitem__(self, key):
        if isinstance(key, slice):
            start, stop, step = key.indices(len(self.image_urls))
            downloaded_paths = []
            for i in range(start, stop, step):
                url = self.image_urls[i]
                self.download_image(url, os.getcwd())
                downloaded_paths.append(os.path.join(os.getcwd(), self.clean_filename(os.path.basename(str(url)))))
            return downloaded_paths
        elif isinstance(key, int):
            if 0 <= key < len(self.image_urls):
                url = self.image_urls[key]
                self.download_image(url, os.getcwd())
                return os.path.join(os.getcwd(), self.clean_filename(os.path.basename(str(url))))
            else:
                raise IndexError("Index out of range")
        else:
            raise TypeError("Invalid index type")

# Example usage:
pq_file_path = r"C:\Users\91812\Downloads\links.parquet"
d = Downloader(pq_file_path)
single_image_path = d[0]  # Download the first image and get its local path
image_paths_list = d[1:4]  # Download images from index 1 to 3 and get their local paths in a list
