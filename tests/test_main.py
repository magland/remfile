import shutil
import os
import time
import numpy as np
import h5py
import remfile

def test_example1():
    url = 'https://dandiarchive.s3.amazonaws.com/blobs/d86/055/d8605573-4639-4b99-a6d9-e0ac13f9a7df'

    file = h5py.File(remfile.File(url))

    assert file.attrs['neurodata_type'] == 'NWBFile'

    dataset = file['/processing/behavior/Whisker_label 1/SpatialSeries/data']
    assert dataset.shape == (217423, 2)

def test_example2(disk_cache=None):
    url = 'https://dandiarchive.s3.amazonaws.com/blobs/c86/cdf/c86cdfba-e1af-45a7-8dfd-d243adc20ced'

    # open the remote file
    f = h5py.File(remfile.File(
        url,
        verbose=True,
        disk_cache=disk_cache,
        _min_chunk_size=100 * 1024,
        _max_cache_size=10 * 1024 * 1024, # low cache size for code coverage
        _chunk_increment_factor=2.5, # high increment factor for code coverage
        _bytes_per_thread=500 * 1024, # low bytes per thread for code coverage
        _max_threads=3, # low max threads for code coverage
        _max_chunk_size=3 * 1024 * 1024, # low max chunk size for code coverage
    ), 'r')

    # load the neurodata object
    X = f['/acquisition/ElectricalSeries']

    starting_time = X['starting_time'][()]
    rate = X['starting_time'].attrs['rate']
    data = X['data']

    print(f'starting_time: {starting_time}')
    print(f'rate: {rate}')
    print(f'data shape: {data.shape}')

    timer = time.time()

    x = data[0:500]

    print(f'Elapsed time: {time.time() - timer} seconds')

    sum0 = np.sum(x)

    print(sum0)
    assert sum0 == 110503440

def test_for_coverage():
    url = 'https://dandiarchive.s3.amazonaws.com/blobs/d86/055/d8605573-4639-4b99-a6d9-e0ac13f9a7df'

    f = remfile.File(url, _impose_request_failures_for_testing=True, verbose=True)

    file = h5py.File(f)

    assert file.attrs['neurodata_type'] == 'NWBFile'

    f.close()

def test_disk_cache():
    tmp_dirname = '/tmp/remfile_test_cache'
    if os.path.exists(tmp_dirname):
        assert tmp_dirname.startswith('/tmp/')
        shutil.rmtree(tmp_dirname)
    disk_cache = remfile.DiskCache(tmp_dirname)

    timer = time.time()
    test_example2(disk_cache=disk_cache)
    elapsed = time.time() - timer
    print(f'Elapsed time for uncached read: {elapsed} seconds')

    # the second time, it should be cached already
    timer = time.time()
    test_example2(disk_cache=disk_cache)
    elapsed = time.time() - timer
    print(f'Elapsed time for cached read: {elapsed} seconds')
    assert elapsed < 2
