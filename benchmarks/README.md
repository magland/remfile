The [benchmarks.py](./benchmarks.py) script was run in a GitHub codespace with environment defined in [.devcontainer/](../.devcontainer)

The results for 11 trials are saved in [results_on_github_codespace.txt](./results_on_github_codespace.txt)

In summary:

**Initial Load Time:**
- fsspec: 8.03 seconds
- ros3: 63.02 seconds
- remfile: 7.29 seconds

**Reading a 30-second chunk of ephys data:**
- fsspec: 7.54 seconds
- ros3: 24.83 seconds
- remfile: 7.56 seconds