import time
import json
import os
import remfile
import fsspec
from pynwb import NWBHDF5IO
import h5py


def main():
    read_nwbfile_examples = [
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

    read_h5_dataset_examples = [
        {
            'name': 'np test1',
            'url': 'https://dandiarchive.s3.amazonaws.com/blobs/eb9/98f/eb998f72-3155-412f-a96a-779aaf1f9a0a',
            'dataset_path': '/acquisition/ElectricalSeriesAp/data',
            'num_timepoints': 30000 * 5
        }
    ]

    results = []

    for example in read_h5_dataset_examples:
        name = example['name']
        url = example['url']
        dataset_path = example['dataset_path']
        num_timepoints = example['num_timepoints']

        print('**********************')
        print(f'Testing read h5 dataset {name}')
        print(f'url: {url}')
        print(f'dataset_path: {dataset_path}')
        print(f'num_timepoints: {num_timepoints}')
        print('')
        for method in ['remfile', 'fsspec', 'ros3']:
            result = _read_h5_dataset_benchmark(url=url, dataset_path=dataset_path, num_timepoints=num_timepoints, method=method)
            results.append(result)
            print('')

    for example in read_nwbfile_examples:
        name = example['name']
        dandiset = example['dandiset']
        version = example['version']
        path = example['path']
        dandiarchive_link = example['dandiarchive_link']
        url = example['url']

        print('**********************')
        print(f'Testing read nwbfile {name}')
        print(f'dandiset: {dandiset}')
        print(f'version: {version}')
        print(f'path: {path}')
        print(f'dandiarchive_link: {dandiarchive_link}')
        print(f'url: {url}')
        print('')
        for method in ['remfile', 'fsspec', 'ros3']:
            result = _read_nwbfile_benchmark(url=url, method=method, dandiset=dandiset, version=version, path=path)
            results.append(result)
            print('')

    results_md = ''
    results_md += '# Results\n\n'
    results_md += '## Read NWBFile\n\n'
    results_md += '| dandiset | version | path | method | elapsed_time_sec |\n'
    results_md += '|----------|--------|------|--------|------------------|\n'
    for result in results:
        if result['type'] == 'read_nwbfile':
            results_md += f"| {result['dandiset']} | {result['version']} | {result['path']} | {result['method']} | {result['elapsed_time_sec']} |\n"
    results_md += '\n\n'
    results_md += '## Read H5 Dataset\n\n'
    results_md += '| name | method | elapsed_time_sec |\n'
    results_md += '|------|--------|------------------|\n'
    for result in results:
        if result['type'] == 'read_h5_dataset':
            results_md += f"| {result['name']} | {result['method']} | {result['elapsed_time_sec']} |\n"
    results_md += '\n\n'

    if not os.path.exists('results'):
        os.makedirs('results')

    with open('results/results.json', 'w') as f:
        f.write(json.dumps(results, indent=4))

    with open('results/results.md', 'w') as f:
        f.write(results_md)

    print(json.dumps(results, indent=4))

    print(results_md)

def _read_nwbfile_benchmark(*, url: str, method: str, dandiset: str, version: str, path: str):
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
        'type': 'read_nwbfile',
        'url': url,
        'method': method,
        'dandiset': dandiset,
        'version': version,
        'path': path,
        'elapsed_time_sec': elapsed_time_sec
    }

def _read_h5_dataset_benchmark(*, url: str, dataset_path: str, num_timepoints: int, method: str):
    print(f'Running benchmark with method: {method}')
    timer = time.time()

    if method == 'remfile':
        file = remfile.File(url)
        h5_file = h5py.File(file, 'r')
    elif method == 'fsspec':
        fs = fsspec.filesystem('http')
        file = fs.open(url)
        h5_file = h5py.File(file, 'r')
    elif method == 'ros3':
        h5_file = h5py.File(url, 'r', driver='ros3')
    else:
        raise Exception(f'Unknown method: {method}')

    dataset = h5_file[dataset_path]
    assert isinstance(dataset, h5py.Dataset)

    print(f'Dataset shape: {dataset.shape}')

    y = dataset[:num_timepoints]
    print(f'Extracted subarray shape: {y.shape}')

    elapsed_time_sec = time.time() - timer
    print(f'Elapsed time: {elapsed_time_sec} seconds')

    return {
        'type': 'read_h5_dataset',
        'url': url,
        'dataset_path': dataset_path,
        'num_timepoints': num_timepoints,
        'method': method,
        'elapsed_time_sec': elapsed_time_sec
    }

if __name__ == '__main__':
    main()
