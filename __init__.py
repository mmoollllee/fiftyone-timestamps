import re
from datetime import datetime

import fiftyone as fo
import fiftyone.operators as foo


def get_filepath(sample):
    return (
        sample.local_path if hasattr(sample, "local_path") else sample.filepath
    )


def timestamp_from_filepath(f, regex):
    matches = re.search(regex, f).groups()
    matches = ['00' if i is None else i for i in matches] # Replace not found items with "01"
    return matches


def compute_timestamps_from_filepath(sample, regex):
    year, month, day, hour, minute, second = timestamp_from_filepath(get_filepath(sample), regex)
    string = f'{year}-{month}-{day}_{hour}-{minute}-{second}'
    dt = datetime.strptime(string, "%Y-%m-%d_%H-%M-%S")
    weekday = dt.weekday()
    time = int(hour) + (int(minute) / 60) + (int(second) / 6000)
    return dt, weekday, time

def compute_timestamps(sample):
    dt = sample.get_field("_id").to_date()
    weekday = dt.weekday()
    time = int(dt.hour) + (int(dt.minute) / 60) + (int(dt.second) / 6000)
    return dt, weekday, time


################################################################
################################################################

class ComputeTimestamps(foo.Operator):
    @property
    def config(self):
        return foo.OperatorConfig(
            name="compute_timestamps",
            label="Compute Timestamps from filepath or sample's creation date",
            description="Filter by various timestamp informations found in filepath or sample`s creation date"
        )


    def execute(self, ctx):
        view = ctx.view
        if view is None:
            view = ctx.dataset

        ctx.dataset.add_sample_field("datetime", fo.DateTimeField)
        ctx.dataset.add_sample_field("weekday", fo.IntField)
        ctx.dataset.add_sample_field("time", fo.FloatField)

        source = ctx.params.get("source")
        regex = ctx.params.get("regex")
        for sample in view.iter_samples(autosave=True, progress=True):
            if source == "created_at":
                dt, weekday, time = compute_timestamps(sample)
            elif source == "filepath" and regex:
                dt, weekday, time = compute_timestamps_from_filepath(sample, regex)
            else:
                return "parameters not allowed"
            sample["datetime"] = dt
            sample["weekday"] = weekday
            sample["time"] = time

    def __call__(self, sample_collection, source="filepath", regex=r".*([0-9]{4})-?([0-9]{2})-?([0-9]{2})_([0-9]{2})?-?([0-9]{2})?-?([0-9]{2})?.*?"):
        ctx = dict(view=sample_collection.view())
        params = dict(
            target="CURRENT_VIEW",
            source=source,
            regex=regex
            )
        return foo.execute_operator(self.uri, ctx, params=params)


def register(p):
    p.register(ComputeTimestamps)
