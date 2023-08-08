from setuptools import setup, find_packages

# read the contents of README.md
from pathlib import Path
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

__version__ = '0.1.4'

setup(
    name='remfile',
    version=__version__,
    author="Jeremy Magland",
    author_email="jmagland@flatironinstitute.org",
    url="https://github.com/magland/remfile",
    description="File-like object from url of remote file, optimized for use with h5py.",
    long_description=long_description,
    long_description_content_type='text/markdown',
    packages=find_packages(),
    install_requires=[
        'numpy',
        'h5py',
        'requests'
    ],
    tests_require=[
        "pytest",
        "pytest-cov"
    ]
)