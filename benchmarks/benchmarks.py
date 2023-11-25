import time
import remfile
import fsspec
from pynwb import NWBHDF5IO
import h5py


def main():
    test_nwb_urls = [
        {
            'name': '000711/sub-403491/sub-403491_ses-20180824T145125_image.nwb',
            'dandiset': '000711',
            'version': '0.231121.1730',
            'path': 'sub-403491/sub-403491_ses-20180824T145125_image.nwb',
            'dandiarchive_link': 'https://dandiarchive.org/dandiset/000711/0.231121.1730/files?location=sub-403491&page=1',
            'url': 'https://dandiarchive.s3.amazonaws.com/blobs/dc0/e33/dc0e33b3-bb55-4a4d-bfdf-e4ec29d988f8'
        },
        {
            'name': '000409/sub-CSHL049/sub-CSHL049_ses-c99d53e6-c317-4c53-99ba-070b26673ac4_behavior+ecephys+image.nwb',
            'dandiset': '000409',
            'version': 'draft',
            'path': 'sub-CSHL049/sub-CSHL049_ses-c99d53e6-c317-4c53-99ba-070b26673ac4_behavior+ecephys+image.nwb',
            'dandiarchive_link': 'https://dandiarchive.org/dandiset/000409/draft/files?location=sub-CSHL049&page=1',
            'url': 'https://dandiarchive.s3.amazonaws.com/blobs/eb9/98f/eb998f72-3155-412f-a96a-779aaf1f9a0a'
        }
    ]

    results = []

    for test_nwb_url in test_nwb_urls:
        name = test_nwb_url['name']
        dandiset = test_nwb_url['dandiset']
        version = test_nwb_url['version']
        path = test_nwb_url['path']
        dandiarchive_link = test_nwb_url['dandiarchive_link']
        url = test_nwb_url['url']

        print('**********************')
        print(f'Testing {name}')
        print(f'dandiset: {dandiset}')
        print(f'version: {version}')
        print(f'path: {path}')
        print(f'dandiarchive_link: {dandiarchive_link}')
        print(f'url: {url}')
        print('')
        for method in ['remfile', 'fsspec', 'ros3']:
            result = _run_benchmark_test(url, method)
            results.append(result)

    with open('results.json', 'w') as f:
        f.write(str(results))

def _run_benchmark_test(url: str, method: str):
    print(f'Running benchmark with method: {method}')
    timer = time.time()

    if method == 'remfile':
        file = remfile.File(url)
        h5_file = h5py.File(file, 'r')
        nwbfile = NWBHDF5IO(file=h5_file, mode='r', load_namespaces=True).read() # noqa
    elif method == 'fsspec':
        fs = fsspec.filesystem('http')
        file = fs.open(url)
        h5_file = h5py.File(file, 'r')
        nwbfile = NWBHDF5IO(file=h5_file, mode='r', load_namespaces=True).read() # noqa
    elif method == 'ros3':
        nwbfile = NWBHDF5IO(path=url, mode='r', load_namespaces=True, driver='ros3') # noqa
    else:
        raise Exception(f'Unknown method: {method}')

    elapsed_time_sec = time.time() - timer
    print(f'Elapsed time: {elapsed_time_sec} seconds')

    return {
        'url': url,
        'method': method,
        'elapsed_time_sec': elapsed_time_sec
    }

if __name__ == '__main__':
    main()
