import os

def create_files(num_files=100, file_size=1024*100, directory='upload_files'):
    os.makedirs(directory, exist_ok=True)
    for i in range(1, num_files + 1):
        file_path = os.path.join(directory, f'file{i}.txt')
        with open(file_path, 'wb') as f:
            f.write(os.urandom(file_size))

create_files()
