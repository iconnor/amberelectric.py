from typing import List, Optional, Union
import json
from dateutil import tz, parser
from datetime import date
import pandas as pd
import numpy as np

from amberelectric.rest import RESTResponse
from amberelectric.configuration import Configuration
from amberelectric.model.site import Site
from amberelectric.model.current_interval import CurrentInterval
from amberelectric.model.actual_interval import ActualInterval
from amberelectric.model.forecast_interval import ForecastInterval
from amberelectric.model.usage import Usage
from amberelectric.model.tariff_information import TariffInformation
from amberelectric.model.channel import Channel, ChannelType
from amberelectric.model.range import Range
from amberelectric.exceptions import ApiException

from ..rest import RESTClientObject
AMBER_DATE_FORMAT = '%Y-%m-%d'


def parse_tariff_information(tariff_information: Optional[object]) -> TariffInformation:
    if tariff_information is None:
        return None
    else:
        return TariffInformation(**tariff_information)


def parse_range(range: Optional[object]) -> Range:
    if range is None:
        return None
    else:
        return Range(float(range['min']), float(range['max']))


def parse_interval(interval: object) -> Union[ActualInterval, CurrentInterval, ForecastInterval, Usage]:
    optional = {}

    if 'tariffInformation' in interval:
        optional['tariff_information'] = parse_tariff_information(interval['tariffInformation'])

    if 'range' in interval:
        optional['range'] = parse_range(interval['range'])

    if interval['type'] == 'ActualInterval':
        return ActualInterval(
            float(interval['duration']),
            float(interval['spotPerKwh']),
            float(interval['perKwh']),
            parser.isoparse(interval['date']).date(),
            parser.isoparse(interval['nemTime']),
            parser.isoparse(interval['startTime']),
            parser.isoparse(interval['endTime']),
            float(interval['renewables']),
            interval['channelType'],
            interval['spikeStatus'],
            interval['descriptor'],
            **optional
        )

    if interval['type'] == 'CurrentInterval':
        return CurrentInterval(
            float(interval['duration']),
            float(interval['spotPerKwh']),
            float(interval['perKwh']),
            parser.isoparse(interval['date']).date(),
            parser.isoparse(interval['nemTime']),
            parser.isoparse(interval['startTime']),
            parser.isoparse(interval['endTime']),
            float(interval['renewables']),
            interval['channelType'],
            interval['spikeStatus'],
            interval['descriptor'],
            interval['estimate'],
            **optional
        )

    if interval['type'] == 'ForecastInterval':
        return ForecastInterval(
            float(interval['duration']),
            float(interval['spotPerKwh']),
            float(interval['perKwh']),
            parser.isoparse(interval['date']).date(),
            parser.isoparse(interval['nemTime']),
            parser.isoparse(interval['startTime']),
            parser.isoparse(interval['endTime']),
            float(interval['renewables']),
            interval['channelType'],
            interval['spikeStatus'],
            interval['descriptor'],
            **optional
        )

    if interval['type'] == 'Usage':
        return Usage(
            float(interval['duration']),
            float(interval['spotPerKwh']),
            float(interval['perKwh']),
            parser.isoparse(interval['date']).date(),
            parser.isoparse(interval['nemTime']),
            parser.isoparse(interval['startTime']),
            parser.isoparse(interval['endTime']),
            float(interval['renewables']),
            interval['channelType'],
            interval['spikeStatus'],
            interval['descriptor'],
            interval['channelIdentifier'],
            float(interval['kwh']),
            interval['quality'],
            float(interval['cost']),
            **optional
        )


def parse_intervals(intervals: List[object]) -> List[Union[ActualInterval, CurrentInterval, ForecastInterval, Usage]]:
    return list(map(parse_interval, intervals))


def parse_channel(channel: object) -> Channel:
    return Channel(channel['identifier'], channel['type'], channel['tariff'])


def parse_channels(channels: List[object]) -> List[Channel]:
    return list(map(parse_channel, channels))


def parse_site(site: object) -> Site:
    return Site(site['id'], site['nmi'], parse_channels(site['channels']))


def parse_sites(sites: List[object]) -> List[Site]:
    return list(map(parse_site, sites))


class AmberApi:
    def __init__(self, rest_client: RESTClientObject):
        self._rest_client = rest_client

    def create(configuration: Configuration):
        return AmberApi(RESTClientObject(configuration))

    def request(self, method, path, query_params=None, headers=None,
                body=None, post_params=None, _preload_content=True,
                _request_timeout=None) -> RESTResponse:
        auth = self._rest_client.configuration.auth_settings()
        if 'apiKey' in auth:
            token = auth['apiKey']
            if headers is None:
                headers = {}
            headers[token['key']] = token['value']

        url = self._rest_client.configuration.host + path
        return self._rest_client.request(method, url, query_params, headers, body, post_params, _preload_content, _request_timeout)

    def get_sites(self) -> List[Site]:
        response = self.request("GET", "/sites")
        if response.status == 200:
            return parse_sites(json.loads(response.data.decode("utf-8")))
        else:
            raise ApiException(response.status, response.reason, response)

    def get_current_price(self, site_id: str, **kwargs) -> List[Union[ActualInterval, CurrentInterval, ForecastInterval]]:
        query_params = {}
        if "resolution" in kwargs:
            query_params["resolution"] = str(kwargs.get("resolution"))
        if "previous" in kwargs:
            query_params["previous"] = str(kwargs.get("previous"))
        if "next" in kwargs:
            query_params["next"] = str(kwargs.get("next"))

        if query_params == {}:
            query_params = None
        response = self.request("GET", "/sites/" + site_id + "/prices/current", query_params)

        if response.status == 200:
            return parse_intervals(json.loads(response.data.decode("utf-8")))
        else:
            raise ApiException(response.status, response.reason, response)

    def get_prices(self, site_id: str, **kwargs) -> List[Union[ActualInterval, CurrentInterval, ForecastInterval]]:
        query_params = {}
        if "end_date" in kwargs:
            query_params["endDate"] = kwargs.get("end_date").strftime(AMBER_DATE_FORMAT)
        if "start_date" in kwargs:
            query_params["startDate"] = kwargs.get("start_date").strftime(AMBER_DATE_FORMAT)
        if "resolution" in kwargs:
            query_params["resolution"] = str(kwargs.get("resolution"))

        if query_params == {}:
            query_params = None

        response = self.request("GET", "/sites/" + site_id + "/prices", query_params)

        if response.status == 200:
            return parse_intervals(json.loads(response.data.decode("utf-8")))
        else:
            raise ApiException(response.status, response.reason, response)

    def get_usage(self, site_id: str, start_date: date, end_date: date, **kwargs) -> List[Usage]:
        query_params = {'startDate': start_date.strftime(AMBER_DATE_FORMAT), 'endDate': end_date.strftime(AMBER_DATE_FORMAT)}
        if "resolution" in kwargs:
            query_params["resolution"] = str(kwargs.get("resolution"))

        response = self.request("GET", "/sites/" + site_id + "/usage", query_params)

        if response.status == 200:
            return parse_intervals(json.loads(response.data.decode("utf-8")))
        else:
            raise ApiException(response.status, response.reason, response)

    def get_usage_dateframe(self, site_id: str, start_date: date, end_date: date, **kwargs):
        response = self.get_usage(site_id, start_date, end_date, **kwargs)
        df = pd.DataFrame([row.to_dict() for row in response])
        if df.empty:
            raise Exception(f"No data found for site {site_id}")

        timezone = kwargs.get("timezone", "Australia/Brisbane")

        # Parse times and convert to local tz
        df["nem_time"] = pd.to_datetime(df["nem_time"], utc=True).dt.tz_convert(tz.gettz(timezone))
        df["start_time"] = pd.to_datetime(df["start_time"], utc=True).dt.tz_convert(tz.gettz(timezone))
        df["end_time"] = pd.to_datetime(df["end_time"], utc=True).dt.tz_convert(tz.gettz(timezone))

        # Deterministic ordering so "first" is stable
        sort_cols = ["nem_time"]
        if "channel_identifier" in df.columns:
            sort_cols.append("channel_identifier")
        df = df.sort_values(sort_cols)

        # Aggregate one bucket of rows (same nem_time) into one row
        def _agg_interval(g: pd.DataFrame) -> pd.Series:
            kwh = g["kwh"].sum(min_count=1)
            cost = g["cost"].sum(min_count=1)

            tariff = np.nan
            if pd.notna(kwh) and kwh != 0 and pd.notna(cost):
                tariff = cost / kwh

            # spot_per_kwh: kwh-weighted average if possible, else first non-null
            spot = np.nan
            if "spot_per_kwh" in g.columns:
                s = g["spot_per_kwh"]
                if s.notna().any():
                    w = g["kwh"].abs()
                    if w.notna().any() and w.sum() != 0:
                        spot = (s.fillna(0) * w.fillna(0)).sum() / w.fillna(0).sum()
                    else:
                        spot = s.dropna().iloc[0]

            def first_non_null(col: str):
                if col not in g.columns:
                    return np.nan
                s = g[col].dropna()
                return s.iloc[0] if len(s) else np.nan

            return pd.Series(
                {
                    "duration": first_non_null("duration"),
                    "date": first_non_null("date"),
                    "start_time": first_non_null("start_time"),
                    "end_time": first_non_null("end_time"),
                    "renewables": first_non_null("renewables"),
                    "quality": first_non_null("quality"),
                    "spike_status": first_non_null("spike_status"),
                    "descriptor": first_non_null("descriptor"),
                    "range": first_non_null("range"),
                    "tariff_information": first_non_null("tariff_information"),
                    "kwh": kwh,
                    "cost": cost,
                    "tariff": tariff,
                    "spot_per_kwh": spot,
                }
            )

        # Build three import buckets and one export bucket
        feed_in = df[df["channel_type"] == ChannelType.FEED_IN].copy()
        general = df[df["channel_type"] == ChannelType.GENERAL].copy()
        controlled = df[df["channel_type"] == ChannelType.CONTROLLED_LOAD].copy()

        feed_agg = feed_in.groupby("nem_time", as_index=True).apply(_agg_interval)
        gen_agg = general.groupby("nem_time", as_index=True).apply(_agg_interval)
        if not(controlled.empty):
            ctl_agg = controlled.groupby("nem_time", as_index=True).apply(_agg_interval)

        # Rename value columns
        meta_cols = [
            "duration",
            "date",
            "start_time",
            "end_time",
            "renewables",
            "quality",
            "spike_status",
            "descriptor",
            "range",
            "tariff_information",
            "spot_per_kwh",
        ]

        feed_agg = feed_agg.rename(
            columns={"kwh": "feed_in_kwh", "cost": "feed_in_cost", "tariff": "feed_in_tariff"}
        )
        gen_agg = gen_agg.rename(
            columns={"kwh": "general_kwh", "cost": "general_cost", "tariff": "general_tariff"}
        )
        if not(controlled.empty):
            ctl_agg = ctl_agg.rename(
                columns={
                    "kwh": "controlled_load_kwh",
                    "cost": "controlled_load_cost",
                    "tariff": "controlled_load_tariff",
                }
            )

        # Assemble metadata from whichever bucket has it (prefer general, then controlled, then feed_in)
        meta = gen_agg[meta_cols].combine_first(feed_agg[meta_cols])
        if not(controlled.empty):
            meta = meta.combine_first(ctl_agg[meta_cols])

        # Assemble values
        out = meta.join(
            feed_agg.drop(columns=meta_cols, errors="ignore"),
            how="outer",
        ).join(
            gen_agg.drop(columns=meta_cols, errors="ignore"),
            how="outer",
        )
        if not(controlled.empty):
            out = out.join(
                ctl_agg.drop(columns=meta_cols, errors="ignore"),
                how="outer",
            )
        out = out.sort_index()

        # Import total columns (GENERAL + CONTROLLED_LOAD)
        out["import_kwh"] = out["general_kwh"].fillna(0)
        if not(controlled.empty):
            out["import_kwh"] = out["import_kwh"] + out["controlled_load_kwh"].fillna(0)
        out["import_cost"] = out["general_cost"].fillna(0)
        if not(controlled.empty):
            out["import_cost"] = out["import_cost"] + out["controlled_load_cost"].fillna(0)
        out["import_tariff"] = np.where(
            out["import_kwh"] != 0,
            out["import_cost"] / out["import_kwh"],
            np.nan,
        )

        # If you prefer NaN (instead of 0) when both components are missing:
        if not(controlled.empty):
            both_missing = out["general_kwh"].isna() & out["controlled_load_kwh"].isna()
            out.loc[both_missing, ["import_kwh", "import_cost", "import_tariff"]] = np.nan

        return out
