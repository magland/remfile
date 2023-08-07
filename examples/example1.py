import time
import h5py
import remfile

url = 'https://dandiarchive.s3.amazonaws.com/blobs/d86/055/d8605573-4639-4b99-a6d9-e0ac13f9a7df'

file = remfile.File(url)

timer = time.time()

with h5py.File(file, 'r') as f:
    print(f['/'].keys())

    dataset = f['/processing/behavior/Whisker_label 1/SpatialSeries/data']
    print(dataset.shape)

    print('Reading data...')
    data = dataset[:]
    print(data.shape)
    print('Done reading data.')

print('Total time: %.2f seconds' % (time.time() - timer))

# on my machine this was ~6 seconds
