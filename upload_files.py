import requests
import os
import mimetypes
import sys

def upload_files(url, directory='upload_files', strategy='random'):
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        mime_type = mimetypes.guess_type(file_path)[0] or 'application/octet-stream'

        with open(file_path, 'rb') as file:
            payload = {'strategy': strategy}
            files = {'file': (filename, file, mime_type)}
            response = requests.post(url, data=payload, files=files)
            print(f"Uploaded {filename}: {response.status_code} - {response.text}")

if __name__ == "__main__":
    url = 'http://localhost:5555/files'  # Update with your actual endpoint

    # Command-line argument for strategy
    if len(sys.argv) > 1:
        strategy = sys.argv[1]
    else:
        strategy = 'random'  # Default strategy

    upload_files(url, strategy=strategy)
