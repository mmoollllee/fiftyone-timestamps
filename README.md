## FiftyOne Timestamp Filters

This plugin provides operators to compute datetime related fields based on samples' filepath.

## Installation

```shell
fiftyone plugins download https://github.com/mmoollllee/fiftyone-timestamps/
```

## Python SDK

You can use the compute operators from the Python SDK!

```python
import fiftyone as fo
import fiftyone.operators as foo

dataset = fo.load_dataset("quickstart")

compute_timestamps = foo.get_operator("@mmoollllee/timestamps/compute_timestamps_from_filepath")

## Compute the brightness of all images in the dataset
compute_timestamps(dataset)
```
