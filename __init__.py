import re
from datetime import datetime

import fiftyone as fo
import fiftyone.operators as foo


def get_filepath(sample):
    return (
        sample.local_path if hasattr(sample, "local_path") else sample.filepath
    )


def timestamp_from_filepath(f):
    matches = re.search(r".*([0-9]{4})-?([0-9]{2})-?([0-9]{2})_([0-9]{2})?-?([0-9]{2})?-?([0-9]{2})?.*?", f).groups()
    matches = ['00' if i is None else i for i in matches] # Replace not found items with "01"
    return matches


def compute_timestamps_from_filepath(sample):
    year, month, day, hour, minute, second = timestamp_from_filepath(get_filepath(sample))
    string = f'{year}-{month}-{day}_{hour}-{minute}-{second}'
    dt = datetime.strptime(string, "%Y-%m-%d_%H-%M-%S")
    weekday = dt.weekday()
    time = int(hour) + (int(minute) / 60) + (int(second) / 6000)
    return dt, weekday, time


################################################################
################################################################

class ComputeTimestampsFromFilepath(foo.Operator):
    @property
    def config(self):
        return foo.OperatorConfig(
            name="compute_timestamps_from_filepath",
            label="Compute Timestamps from Filepath",
            description="Filter by various timestamp informations found in filepath"
        )


    def execute(self, ctx):
        view = ctx.view
        if view is None:
            view = ctx.dataset

        ctx.dataset.add_sample_field("datetime", fo.DateTimeField)
        ctx.dataset.add_sample_field("weekday", fo.IntField)
        ctx.dataset.add_sample_field("time", fo.FloatField)

        for sample in view.iter_samples(autosave=True, progress=True):
            dt, weekday, time = compute_timestamps_from_filepath(sample)
            sample["datetime"] = dt
            sample["weekday"] = weekday
            sample["time"] = time

    def __call__(self, sample_collection):
        ctx = dict(view=sample_collection.view())
        params = dict(target="CURRENT_VIEW")
        return foo.execute_operator(self.uri, ctx, params=params)


def register(p):
    p.register(ComputeTimestampsFromFilepath)
