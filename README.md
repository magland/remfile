# remfile

Provides a file-like object for reading a remote file over HTTP, optimized for use with h5py.

Example usage:

```python
# See examples/example1.py

import h5py
import remfile

url = 'https://dandiarchive.s3.amazonaws.com/blobs/d86/055/d8605573-4639-4b99-a6d9-e0ac13f9a7df'

file = remfil.File(url)

with h5py.File(file, 'r') as f:
    print(f['/'].keys())
```

See [examples/example1.py](examples/example1.py) for a more complete example.

## Installation

```bash
pip install remfile
```

## Why?

The conventional way of reading a remote hdf5 file is to use the fsspec library as in [examples/example1_compare_fsspec.py](examples/example1_compare_fsspec.py). However, this approach is empirically much slower than using remfile. I am not familiar with the inner workings of fsspec, but it does not seem to be optimized for reading hdf5 files. Efficient access of remote hdf5 files requires reading small chunks of data to obtain meta information, and then large chunks of data to obtain the larger data arrays.

See a timing comparison betweeen remfile and fsspec in the examples directory.

## How?

A file-like object is created that reads the remote file in chunks using the requests library. A relatively small default chunk size is used, but when the system detects that a large data array is being accessed, it switches to a larger chunk size.

## Caveats

This library is not intended to be a general purpose library for reading remote files. It is optimized for reading hdf5 files.

## License

Apache 2.0

## Author

Jeremy Magland, Center for Computational Mathematics, Flatiron Institute
