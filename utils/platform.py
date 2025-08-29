import os

# A Generator to read a file in chunks
def chunk_read(file_path: str, chunk_size: int = 1024 * 1024):
    """
    Generator to read a file in chunks of specified size.

    Args:
        file_path (str): Path to the file to be read.
        chunk_size (int): Size of each chunk in bytes. Default is 1 MB.

    Yields:
        bytes: A chunk of the file.
    """
    with open(file_path, 'rb') as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            yield chunk
