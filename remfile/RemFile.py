from typing import Union
import time
from concurrent.futures import ThreadPoolExecutor
import requests
from .DiskCache import DiskCache

default_min_chunk_size = 100 * 1024
default_max_cache_size = 1e8
default_chunk_increment_factor = 1.7
default_bytes_per_thread = 4 * 1024 * 1024
default_max_threads = 3

class RemFile:
    def __init__(self,
        url: str, *,
        verbose: bool=False,
        disk_cache: Union[DiskCache, None]=None,
        _min_chunk_size: int=default_min_chunk_size,
        _max_cache_size: int=default_max_cache_size,
        _chunk_increment_factor: int=default_chunk_increment_factor,
        _bytes_per_thread: int=default_bytes_per_thread,
        _max_threads: int=default_max_threads,
        _max_chunk_size: int=100 * 1024 * 1024,
        _impose_request_failures_for_testing: bool=False
    ):
        """Create a file-like object for reading a remote file. Optimized for reading hdf5 files. The arguments starting with an underscore are for testing and debugging purposes - they may experience breaking changes in the future.

        Args:
            url (str): The url of the remote file.
            verbose (bool, optional): Whether to print info for debugging. Defaults to False.
            disk_cache (DiskCache, optional): A disk cache for storing the chunks of the file. Defaults to None.
            _min_chunk_size (int, optional): The minimum chunk size. When reading, the chunks will be loaded in multiples of this size.
            _max_cache_size (int, optional): The maximum number of bytes to keep in the cache.
            _chunk_increment_factor (int, optional): The factor by which to increase the number of chunks to load when the system detects that the chunks are being loaded in order.
            _bytes_per_thread (int, optional): The minimum number of bytes to load in each thread.
            _max_threads (int, optional): The maximum number of threads to use when loading the file.
            _max_chunk_size (int, optional): The maximum chunk size. When reading, the chunks will be loaded in multiples of the minimum chunk size up to this size.
            _impose_request_failures_for_testing (bool, optional): Whether to impose request failures for testing purposes. Defaults to False.
        """
        self._url = url
        self._verbose = verbose
        self._disk_cache = disk_cache
        self._min_chunk_size = _min_chunk_size
        self._max_chunks_in_cache = int(_max_cache_size / _min_chunk_size)
        self._chunk_increment_factor = _chunk_increment_factor
        self._bytes_per_thread = _bytes_per_thread
        self._max_threads = _max_threads
        self._max_chunk_size = _max_chunk_size
        self._impose_request_failures_for_testing = _impose_request_failures_for_testing
        self._chunks = {}
        self._chunk_indices: list[int] = [] # list of chunk indices in order of loading for purposes of cleaning up the cache
        self._position = 0
        self._smart_loader_last_chunk_index_accessed = -99
        self._smart_loader_chunk_sequence_length = 1
        response = requests.head(self._url)
        self.length = int(response.headers['Content-Length'])

    def read(self, size=None):
        """Read bytes from the file.

        Args:
            size (_type_): The number of bytes to read.

        Raises:
            Exception: If the size argument is not provided.

        Returns:
            bytes: The bytes read.
        """
        if size is None:
            raise Exception('The size argument must be provided in remfile') # pragma: no cover
        
        chunk_start_index = self._position // self._min_chunk_size
        chunk_end_index = (self._position + size - 1) // self._min_chunk_size
        for chunk_index in range(chunk_start_index, chunk_end_index + 1):
            self._load_chunk(chunk_index)
        if chunk_end_index == chunk_start_index:
            chunk = self._chunks[chunk_start_index]
            chunk_offset = self._position % self._min_chunk_size
            chunk_length = size
            self._position += size
            return chunk[chunk_offset:chunk_offset + chunk_length]
        else:
            pieces_to_concat = []
            for chunk_index in range(chunk_start_index, chunk_end_index + 1):
                chunk = self._chunks[chunk_index]
                if chunk_index == chunk_start_index:
                    chunk_offset = self._position % self._min_chunk_size
                    chunk_length = self._min_chunk_size - chunk_offset
                elif chunk_index == chunk_end_index:
                    chunk_offset = 0
                    chunk_length = size - sum([len(p) for p in pieces_to_concat])
                else:
                    chunk_offset = 0
                    chunk_length = self._min_chunk_size
                pieces_to_concat.append(chunk[chunk_offset:chunk_offset + chunk_length])
        ret = b''.join(pieces_to_concat)
        self._position += size

        # clean up the cache
        if len(self._chunk_indices) > self._max_chunks_in_cache:
            if self._verbose:
                print("Cleaning up cache")
            for chunk_index in self._chunk_indices[:int(self._max_chunks_in_cache * 0.5)]:
                del self._chunks[chunk_index]
            self._chunk_indices = self._chunk_indices[int(self._max_chunks_in_cache * 0.5):]

        return ret
    
    def _load_chunk(self, chunk_index: int):
        """Load a chunk of the file.

        Args:
            chunk_index (int): The index of the chunk to load.
        """
        if chunk_index in self._chunks:
            self._smart_loader_last_chunk_index_accessed = chunk_index
            return
        
        if self._disk_cache:
            kk = _key_for_disk_cache(self._url, self._min_chunk_size, chunk_index)
            cached_value = self._disk_cache.get(kk)
            if cached_value:
                self._chunks[chunk_index] = cached_value
                self._chunk_indices.append(chunk_index)
                self._smart_loader_last_chunk_index_accessed = chunk_index
                return

        if chunk_index == self._smart_loader_last_chunk_index_accessed + 1:
            # round up to the chunk sequence length times 1.7
            self._smart_loader_chunk_sequence_length = round(self._smart_loader_chunk_sequence_length * 1.7 + 0.5)
            if self._smart_loader_chunk_sequence_length > self._max_chunk_size / self._min_chunk_size:
                self._smart_loader_chunk_sequence_length = int(self._max_chunk_size / self._min_chunk_size)
            # make sure the chunk sequence length is valid
            for j in range(1, self._smart_loader_chunk_sequence_length):
                if chunk_index + j in self._chunks:
                    # already loaded this chunk
                    self._smart_loader_chunk_sequence_length = j
                    break
        else:
            self._smart_loader_chunk_sequence_length = 1
        data_start = chunk_index * self._min_chunk_size
        data_end = data_start + self._min_chunk_size * self._smart_loader_chunk_sequence_length - 1
        if self._verbose:
            print(f"Loading {self._smart_loader_chunk_sequence_length} chunks starting at {chunk_index} ({(data_end - data_start + 1)/1e6} million bytes)")
        if data_end >= self.length:
            data_end = self.length - 1
        x = _get_bytes(
            self._url,
            data_start,
            data_end,
            verbose=self._verbose,
            bytes_per_thread=self._bytes_per_thread,
            max_threads=self._max_threads,
            _impose_request_failures_for_testing=self._impose_request_failures_for_testing
        )
        if self._smart_loader_chunk_sequence_length == 1:
            self._chunks[chunk_index] = x
            if self._disk_cache:
                self._disk_cache.set(_key_for_disk_cache(self._url, self._min_chunk_size, chunk_index), self._chunks[chunk_index])
            self._chunk_indices.append(chunk_index)
        else:
            for i in range(self._smart_loader_chunk_sequence_length):
                self._chunks[chunk_index + i] = x[i * self._min_chunk_size:(i + 1) * self._min_chunk_size]
                if self._disk_cache:
                    self._disk_cache.set(_key_for_disk_cache(self._url, self._min_chunk_size, chunk_index + i), self._chunks[chunk_index + i])
                self._chunk_indices.append(chunk_index + i)
        self._smart_loader_last_chunk_index_accessed = chunk_index + self._smart_loader_chunk_sequence_length - 1

    def seek(self, offset: int, whence: int=0):
        """Seek to a position in the file.

        Args:
            offset (int): The offset to seek to.
            whence (int, optional): The code for the reference point for the offset. Defaults to 0.

        Raises:
            ValueError: If the whence argument is not 0, 1, or 2.
        """
        if whence == 0:
            self._position = offset
        elif whence == 1:
            self._position += offset # pragma: no cover
        elif whence == 2:
            self._position = self.length + offset
        else:
            raise ValueError("Invalid argument: 'whence' must be 0, 1, or 2.") # pragma: no cover

    def tell(self):
        return self._position

    def close(self):
        pass

def _key_for_disk_cache(url: str, min_chunk_size: int, chunk_index: int):
    return f'{url}|{min_chunk_size}|{chunk_index}'

_num_request_retries = 8

def _get_bytes(url: str, start_byte: int, end_byte: int, *, verbose=False, bytes_per_thread: int, max_threads: int, _impose_request_failures_for_testing=False):
    """Get bytes from a remote file.

    Args:
        url (str): The url of the remote file.
        start_byte (int): The first byte to get.
        end_byte (int): The last byte to get.
        bytes_per_thread (int): The minimum number of bytes to load in each thread.
        max_threads (int): The maximum number of threads to use when loading the file.
        verbose (bool, optional): Whether to print info for debugging. Defaults to False.

    Returns:
        _type_: _description_
    """
    num_bytes = end_byte - start_byte + 1

    # Function to be used in threads for fetching the byte ranges
    def fetch_bytes(range_start: int, range_end: int, num_retries: int, verbose: bool):
        """Fetch a range of bytes from a remote file using the range header

        Args:
            range_start (int): The first byte to get.
            range_end (int): The last byte to get.
            num_retries (int): The number of retries.

        Returns:
            bytes: The bytes fetched.
        """
        for try_num in range(num_retries + 1):
            try:
                actual_url = url
                if _impose_request_failures_for_testing:
                    if try_num == 0:
                        actual_url = '_error_' + url
                range_header = f"bytes={range_start}-{range_end}"
                response = requests.get(actual_url, headers={'Range': range_header})
                return response.content
            except Exception as e:
                if try_num == num_retries:
                    raise e # pragma: no cover
                else:
                    delay = 0.1 * 2 ** try_num
                    if verbose:
                        print(f"Retrying after exception: {e}")
                        print(f'Waiting {delay} seconds')
                    time.sleep(delay)

    if num_bytes < bytes_per_thread * 2:
        # If the number of bytes is less than 2 times the bytes_per_thread,
        # then we can just use a single thread
        return fetch_bytes(start_byte, end_byte, _num_request_retries, verbose)
    else:
        num_threads = num_bytes // bytes_per_thread
        if num_threads > max_threads:
            num_threads = max_threads
        thread_args = []
        a = start_byte
        for i in range(num_threads):
            if i == num_threads - 1:
                b = end_byte
            else:
                b = a + num_bytes // num_threads - 1
            thread_args.append((a, b, _num_request_retries, verbose))
            a = b + 1
        
        if verbose:
            print(f"Fetching {num_bytes} bytes in {num_threads} threads")

        # Using ThreadPoolExecutor to manage the threads
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            # Mapping fetch_bytes function to the byte_ranges
            results = list(executor.map(lambda r: fetch_bytes(*r), thread_args))

        # Concatenating the results to form the final content
        final_content = b''.join(results)
        return final_content
