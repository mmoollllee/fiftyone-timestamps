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

compute_timestamps = foo.get_operator("@mmoollllee/timestamps/compute_timestamps")

## Compute from filepath with custom regex pattern where filenames look like `image-2024-12-30_23-59-59.jpg`
compute_timestamps(dataset, source="filepath", regex=r".*([0-9]{4})-?([0-9]{2})-?([0-9]{2})_([0-9]{2})?-?([0-9]{2})?-?([0-9]{2})?.*?")

## Compute from created_at
compute_timestamps(dataset, source="created_at")

## If geo[lat, long] is set, `timeofday` will be computed with "sunrise", "morning", "day", "evening", "sunset", "night"
compute_timestamps(dataset, geo=[48.12345,9.12345], tz="Europe/Berlin")
```

## ToDos
- [x] Implement `timeofday` field with Daytime / Nighttime / Morning / Evening by using [suntime](https://github.com/SatAgro/suntime)
