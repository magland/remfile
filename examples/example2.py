import time
import numpy as np
import h5py
import remfile

url = 'https://dandiarchive.s3.amazonaws.com/blobs/c86/cdf/c86cdfba-e1af-45a7-8dfd-d243adc20ced'

# open the remote file
f = h5py.File(remfile.File(url, verbose=True), 'r')

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