import re
from datetime import datetime, timedelta
from dateutil import tz
from dateutil.tz.tz import tzfile
from typing import Optional
from suntime import Sun

import fiftyone as fo
import fiftyone.operators as foo


def get_filepath(sample):
    return (
        sample.local_path if hasattr(sample, "local_path") else sample.filepath
    )


def timestamp_from_filepath(f, regex):
    matches = re.search(regex, f).groups()
    matches = ['00' if i is None else i for i in matches] # Replace not found items with "00"
    return matches

def get_timeofday(
        dt: datetime,
        geo: list,
        tzinfo: tzfile,
        sunrise_length: timedelta = timedelta(minutes=10),
        sunset_length: timedelta = timedelta(minutes=10),
        morning_length: timedelta = timedelta(hours=1),
        evening_length: timedelta = timedelta(hours=1)
    ):
    sun = Sun(*geo)
    sunrise = sun.get_sunrise_time(dt, tzinfo)
    morning_end = sunrise + morning_length
    sunset = sun.get_sunset_time(dt, tzinfo)
    evening_begin = sunset - evening_length

    if dt <= sunrise - sunrise_length:
        return "night"
    elif dt <= sunrise + sunrise_length:
        return "sunrise"
    elif dt <= morning_end:
        return "morning"
    elif dt < evening_begin:
        return "day"
    elif dt < sunset - sunset_length:
        return "evening"
    elif dt <= sunset + sunset_length:
        return "sunset"
    else:
        return "night"


def compute_timestamps_from_filepath(sample, regex, tzinfo, geo=None):
    year, month, day, hour, minute, second = timestamp_from_filepath(get_filepath(sample), regex)
    string = f'{year}-{month}-{day}_{hour}-{minute}-{second}'
    dt = datetime.strptime(string, "%Y-%m-%d_%H-%M-%S").replace(tzinfo=tzinfo)
    weekday = dt.weekday()
    time = int(hour) + (int(minute) / 60) + (int(second) / 6000)
    
    timeofday = None
    if type(geo) is list:
        timeofday = get_timeofday(dt=dt, geo=geo, tzinfo=tzinfo)

    return dt, weekday, time, timeofday

def compute_timestamps(sample, tzinfo, geo=None):
    dt = sample.get_field("_id").to_date().replace(tzinfo=tzinfo)
    weekday = dt.weekday()
    time = int(dt.hour) + (int(dt.minute) / 60) + (int(dt.second) / 6000)
        
    timeofday = None
    if type(geo) is list:
        timeofday = get_timeofday(dt=dt, geo=geo, tzinfo=tzinfo)

    return dt, weekday, time, timeofday


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

        source = ctx.params.get("source")
        regex = ctx.params.get("regex")
        geo = ctx.params.get("geo")

        tzinfo = tz.gettz(ctx.params.get("timezone"))

        ctx.dataset.add_sample_field("datetime", fo.DateTimeField)
        ctx.dataset.add_sample_field("weekday", fo.IntField)
        ctx.dataset.add_sample_field("time", fo.FloatField)
        if geo:
            ctx.dataset.add_sample_field("timeofday", fo.StringField)

        for sample in view.iter_samples(autosave=True, progress=True):
            if source == "created_at":
                dt, weekday, time, timeofday = compute_timestamps(sample, tzinfo, geo)
            elif source == "filepath" and regex:
                dt, weekday, time, timeofday = compute_timestamps_from_filepath(sample, regex, tzinfo, geo)
            else:
                return "parameters not allowed"
            
            sample["datetime"] = dt
            sample["weekday"] = weekday
            sample["time"] = time
            if geo:
                sample["timeofday"] = timeofday

    def __call__(
        self, 
        sample_collection, 
        source: str = "filepath", 
        regex: str = r".*([0-9]{4})-?([0-9]{2})-?([0-9]{2})_([0-9]{2})?-?([0-9]{2})?-?([0-9]{2})?.*?",
        geo: Optional[list] = None,
        timezone: str = 'Europe/Berlin'
    ):
        ctx = dict(view=sample_collection.view())
        params = dict(
            target="CURRENT_VIEW",
            source=source,
            regex=regex,
            geo=geo,
            timezone=timezone
            )
        return foo.execute_operator(self.uri, ctx, params=params)


def register(p):
    p.register(ComputeTimestamps)
