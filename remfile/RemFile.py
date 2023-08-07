# Here's the much faster alternative

import requests

class RemFile:
    def __init__(self, url, *, verbose=False):
        self._verbose = verbose
        self._chunk_size = 100 * 1024
        self._chunks = {}
        self._url = url
        self._position = 0
        self._smart_loader_last_chunk_index_read = -99
        self._smart_loader_string_length = 1
        self._get_file_length()

    def _get_file_length(self):
        response = requests.head(self._url)
        self.length = int(response.headers['Content-Length'])

    def read(self, size=None):
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
            # round up to the string length times 1.5
            self._smart_loader_string_length = round(self._smart_loader_string_length * 1.5 + 0.5)
            if self._smart_loader_string_length > 15 * 1024 * 1024 / self._chunk_size:
                self._smart_loader_string_length = int(15 * 1024 * 1024 / self._chunk_size)
        else:
            self._smart_loader_string_length = 1
        if self._verbose:
            print(f"Loading chunks {chunk_index} ({self._smart_loader_string_length})")
        data_start = chunk_index * self._chunk_size
        data_end = data_start + self._chunk_size * self._smart_loader_string_length - 1
        if data_end >= self.length:
            data_end = self.length - 1
        range_header = f"bytes={data_start}-{data_end}"
        response = requests.get(self._url, headers={'Range': range_header})
        x = response.content
        if self._smart_loader_string_length == 1:
            self._chunks[chunk_index] = x
        else:
            for i in range(self._smart_loader_string_length):
                self._chunks[chunk_index + i] = x[i * self._chunk_size:(i + 1) * self._chunk_size]
        self._smart_loader_last_chunk_index_read = chunk_index + self._smart_loader_string_length - 1

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