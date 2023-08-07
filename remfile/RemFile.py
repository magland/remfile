from concurrent.futures import ThreadPoolExecutor
import requests

class RemFile:
    def __init__(self, url, *, verbose=False):
        self._verbose = verbose
        self._chunk_size = 100 * 1024
        self._chunks = {}
        self._url = url
        self._position = 0
        self._smart_loader_last_chunk_index_read = -99
        self._smart_loader_chunk_sequence_length = 1
        response = requests.head(self._url)
        self.length = int(response.headers['Content-Length'])

    def read(self, size=None):
        if size is None:
            raise Exception('The size argument must be provided in remfile')
        chunk_start_index = self._position // self._chunk_size
        chunk_end_index = (self._position + size - 1) // self._chunk_size
        for chunk_index in range(chunk_start_index, chunk_end_index + 1):
            self._load_chunk(chunk_index)
        if chunk_end_index == chunk_start_index:
            chunk = self._chunks[chunk_start_index]
            chunk_offset = self._position % self._chunk_size
            chunk_length = size
            self._position += size
            return chunk[chunk_offset:chunk_offset + chunk_length]
        else:
            pieces_to_concat = []
            for chunk_index in range(chunk_start_index, chunk_end_index + 1):
                chunk = self._chunks[chunk_index]
                if chunk_index == chunk_start_index:
                    chunk_offset = self._position % self._chunk_size
                    chunk_length = self._chunk_size - chunk_offset
                elif chunk_index == chunk_end_index:
                    chunk_offset = 0
                    chunk_length = size - sum([len(p) for p in pieces_to_concat])
                else:
                    chunk_offset = 0
                    chunk_length = self._chunk_size
                pieces_to_concat.append(chunk[chunk_offset:chunk_offset + chunk_length])
        ret = b''.join(pieces_to_concat)
        self._position += size
        return ret
    
    def _load_chunk(self, chunk_index):
        if chunk_index in self._chunks:
            return
        if chunk_index == self._smart_loader_last_chunk_index_read + 1:
            # round up to the chunk sequence length times 1.7
            self._smart_loader_chunk_sequence_length = round(self._smart_loader_chunk_sequence_length * 1.7 + 0.5)
            if self._smart_loader_chunk_sequence_length > 15 * 1024 * 1024 / self._chunk_size:
                self._smart_loader_chunk_sequence_length = int(15 * 1024 * 1024 / self._chunk_size)
        else:
            self._smart_loader_chunk_sequence_length = 1
        data_start = chunk_index * self._chunk_size
        data_end = data_start + self._chunk_size * self._smart_loader_chunk_sequence_length - 1
        if self._verbose:
            print(f"Loading chunks starting at {chunk_index} ({(data_end - data_start + 1)/1e6} million bytes)")
        if data_end >= self.length:
            data_end = self.length - 1
        x = _get_bytes(self._url, data_start, data_end, verbose=self._verbose)
        if self._smart_loader_chunk_sequence_length == 1:
            self._chunks[chunk_index] = x
        else:
            for i in range(self._smart_loader_chunk_sequence_length):
                self._chunks[chunk_index + i] = x[i * self._chunk_size:(i + 1) * self._chunk_size]
        self._smart_loader_last_chunk_index_read = chunk_index + self._smart_loader_chunk_sequence_length - 1

    def seek(self, offset, whence=0):
        if whence == 0:
            self._position = offset
        elif whence == 1:
            self._position += offset
        elif whence == 2:
            self._position = self.length + offset
        else:
            raise ValueError("Invalid argument: 'whence' must be 0, 1, or 2.")

    def tell(self):
        return self._position

    def close(self):
        pass

bytes_per_thread = 3 * 1024 * 1024
max_threads = 4

def _get_bytes(url: str, start_byte: int, end_byte: int, verbose=False):
    num_bytes = end_byte - start_byte + 1

    # Function to be used in threads for fetching the byte ranges
    def fetch_bytes(range_start, range_end):
        range_header = f"bytes={range_start}-{range_end}"
        response = requests.get(url, headers={'Range': range_header})
        return response.content

    if num_bytes < bytes_per_thread * 2:
        # If the number of bytes is less than 2 times the bytes_per_thread,
        # then we can just use a single thread
        return fetch_bytes(start_byte, end_byte)
    else:
        num_threads = num_bytes // bytes_per_thread
        if num_threads > max_threads:
            num_threads = max_threads
        byte_ranges = []
        a = start_byte
        for i in range(num_threads):
            if i == num_threads - 1:
                b = end_byte
            else:
                b = a + num_bytes // num_threads - 1
            byte_ranges.append((a, b))
            a = b + 1
        
        if verbose:
            print(f"Fetching {num_bytes} bytes in {num_threads} threads")

        # Using ThreadPoolExecutor to manage the threads
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            # Mapping fetch_bytes function to the byte_ranges
            results = list(executor.map(lambda r: fetch_bytes(*r), byte_ranges))

        # Concatenating the results to form the final content
        final_content = b''.join(results)
        return final_content
