import re
from datetime import datetime, timedelta
from dateutil import tz
from dateutil.tz.tz import tzfile
from typing import Optional
from suntime import Sun

import fiftyone as fo
import fiftyone.operators as foo
from fiftyone import ViewField as F


def timestamp_from_filepath(f: str, regex: str) -> datetime:
    matches = re.search(regex, f).groups()
    matches = ['00' if i is None else i for i in matches] # Replace not found items with "00"
    year, month, day, hour, minute, second = matches
    return datetime.strptime(f'{year}-{month}-{day}_{hour}-{minute}-{second}', "%Y-%m-%d_%H-%M-%S")


def get_timeofday(
        dt: datetime,
        geo: list,
        tzinfo: tzfile,
        dawn_length: timedelta = timedelta(minutes=15),
        dusk_length: timedelta = timedelta(minutes=15),
        sunrise_length: timedelta = timedelta(minutes=15),
        sunset_length: timedelta = timedelta(minutes=15),
        morning_length: timedelta = timedelta(hours=1),
        evening_length: timedelta = timedelta(hours=1)
    ) -> str:
    sun = Sun(*geo)
    sunrise = sun.get_sunrise_time(dt, tzinfo)
    morning_end = sunrise + morning_length
    sunset = sun.get_sunset_time(dt, tzinfo)
    # Fix sunset as stated here: https://github.com/SatAgro/suntime/issues/12#issuecomment-621755084
    if sunset < sunrise:
        sunset = sunset + timedelta(days=1)
    evening_begin = sunset - evening_length

    if dt <= sunrise - sunrise_length - dawn_length:
        return "night"
    elif dt <= sunrise - sunrise_length:
        return "dawn"
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
    elif dt <= sunset + sunset_length + dusk_length:
        return "dusk"
    else:
        return "night"


def compute_timestamps(dt: datetime, tzinfo: tzfile, geo: Optional[list] = None) -> list:
    dt = dt.replace(tzinfo=tzinfo)
    weekday = dt.weekday()
    time = int(dt.hour) + (int(dt.minute) / 60) + (int(dt.second) / 6000)
        
    timeofday = None
    if geo:
        timeofday = get_timeofday(dt=dt, geo=geo, tzinfo=tzinfo)

    return weekday, time, timeofday


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

        dts, weekdays, times, timeofdays = [], [], [], []
        if source == "created_at":
            dts = view.values(F("_id").to_date())
        elif source == "filepath" and regex:
            for filepath in view.values("filepath"):
                dts.append(timestamp_from_filepath(filepath, regex))
        else:
            return "parameters not allowed"

        for dt in dts:
            weekday, time, timeofday = compute_timestamps(dt, tzinfo, geo)
            weekdays.append(weekday)
            times.append(time)
            if geo:
                timeofdays.append(timeofday)

        view.set_values("datetime", dts)
        view.set_values("weekday", weekdays)
        view.set_values("time", times)
        if geo:
            view.set_values("timeofday", timeofdays)

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
