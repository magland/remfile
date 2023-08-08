import os
import hashlib


class DiskCache:
    def __init__(self, dirname: str) -> None:
        """A simple non-lru disk cache.

        Args:
            dirname (str): The directory to use for the cache.
        """
        self._dirname = dirname
    def get(self, key: str):
        h = hashlib.sha1(key.encode('utf-8')).hexdigest()
        p = f'{h[0]}{h[1]}/{h[2]}{h[3]}/{h[4]}{h[5]}/{h}'
        filename = os.path.join(self._dirname, p)
        if not os.path.exists(filename):
            return None
        with open(filename, 'rb') as f:
            return f.read()
    def set(self, key: str, value: bytes):
        h = hashlib.sha1(key.encode('utf-8')).hexdigest()
        p = f'{h[0]}{h[1]}/{h[2]}{h[3]}/{h[4]}{h[5]}/{h}'
        filename = os.path.join(self._dirname, p)
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'wb') as f:
            f.write(value)