import re
import grpc
import logging
import itertools
import time

import esoh.datastore_pb2 as ds
import esoh.datastore_pb2_grpc as ds_grpc

import google.protobuf.struct_pb2 as Struct

from pygeoapi.provider.base import BaseProvider
from pygeoapi.provider.base_edr import BaseEDRProvider
from pygeoapi.util import crs_transform

from google.protobuf import json_format
from google.protobuf.timestamp_pb2 import Timestamp
from datetime import datetime, timezone
from geojson_pydantic import FeatureCollection, Feature, Point

from itertools import groupby

from pydantic import AwareDatetime

from covjson_pydantic.coverage import Coverage, CoverageCollection
from covjson_pydantic.domain import Domain, DomainType, Axes, ValuesAxis
from covjson_pydantic.ndarray import NdArray
from covjson_pydantic.observed_property import ObservedProperty
from covjson_pydantic.parameter import Parameter
from covjson_pydantic.reference_system import ReferenceSystemConnectionObject, ReferenceSystem

LOGGER = logging.getLogger(__name__)


def is_timezone_aware(dt):
    if re.search(r"\+\d{2}:\d{2}$", dt) or re.search(r"\[z,Z]$", dt):
        return True
    else:
        return False


def dtstr2datetime(dtstr):
    if not is_timezone_aware(dtstr):
        dtstr = dtstr + "Z"

    tstamp = datetime.strptime(
        dtstr, "%Y-%m-%dT%H:%M:%S%z")
    return tstamp


def dtime2tstamp(dtime):
    if isinstance(dtime, str):
        dtime = dtstr2datetime(dtime)
    tstamp = Timestamp()
    tstamp.FromDatetime(dtime)
    return tstamp


def fix_timezone(dtstr):
    if not is_timezone_aware:
        return dtstr + "Z"
    return dtstr


def collect_data(ts_mdata, obs_mdata):
    lat = obs_mdata[0].geo_point.lat  # HACK: For now assume they all have the same position
    lon = obs_mdata[0].geo_point.lon
    tuples = ((o.obstime_instant.ToDatetime(tzinfo=timezone.utc), float(o.value))
              for o in obs_mdata)  # HACK: str -> float
    (times, values) = zip(*tuples)
    param_id = ts_mdata.standard_name

    return (lat, lon, times), param_id, values


def unpack_paramaters(coverages):
    paramaters = {}

    for i in map((lambda x: x.parameters), coverages):
        paramaters.update(i)

    return paramaters


class gRPCprovider(BaseProvider):
    """
    Base provider for gRPC service.

    Returns gRPC response and is not intended to be used for
    endpoints.
    """

    def __init__(self, provider_def):
        """
        Docstring for grpcprovider poc
        """
        super().__init__(provider_def)

        options = [('grpc.max_message_length', 100 * 1024 * 1024),
                   ('grpc.max_receive_message_length', 512 * 1024 * 1024)]

        self._channel = grpc.secure_channel(
            f'{provider_def["options"]["dshost"]}:{provider_def["options"]["dsport"]}', credentials=grpc.ssl_channel_credentials(), options=options)
        self._stub = ds_grpc.DatastoreStub(self._channel)

        self.is_channel_ready()

        LOGGER.info(
            f"Established connection to {provider_def['options']['dshost']} gRPC service\n")

        self.fields = self.get_fields()

    def get_grpc(self, obs_req):
        try:
            response = self._stub.GetObservations(obs_req)
        except grpc.RpcError as e:
            LOGGER.critical(e)
            raise
        return response

    def query(self, **kwargs):
        pass

    def is_channel_ready(self):
        try:
            grpc.channel_ready_future(self._channel).result(timeout=10)
        except grpc.FutureTimeoutError:
            LOGGER.exception(grpc.FutureTimeoutError("Connection to the grpc service timed out, "
                                                     "and was not available at application start."))
            raise grpc.FutureTimeoutError("Connection to the grpc service timed out, "
                                          "and was not available at application start.")
        except Exception:
            raise


class gRPC_EDRprovider(BaseEDRProvider, gRPCprovider):
    """Provider for the E-SOH datastore grpc service"""

    def __init__(self, provider_def):
        """
        Docstring for grpcprovider poc
        """
        BaseEDRProvider.__init__(self, provider_def)
        gRPCprovider.__init__(self, provider_def)

        self._coverage_properties = self.get_fields()

    @BaseEDRProvider.register()
    def cube(self, **kwargs):
        LOGGER.debug("Parsing cube EDR query")

        queriables = self._parse_common_queriables(**kwargs)

        inside = queriables.pop("inside") if "inside" in queriables else None
        interval = queriables.pop("interval") if "interval" in queriables else None
        obs_req = ds.GetObsRequest(inside=inside,
                                   interval=interval)

        # All paramaters with depth 1 can be parsed here
        json_format.ParseDict(queriables, obs_req)

        # LOGGER.debug("Observation request", obs_req)

        try:
            response = super().get_grpc(obs_req)
        except grpc.RpcError as e:
            LOGGER.critical(e)
            return {}

        LOGGER.debug("Got observation response")

        coverages = []
        data = [collect_data(md.ts_mdata, md.obs_mdata) for md in response.observations]

        # Need to sort before using groupBy
        data.sort(key=lambda x: x[0])
        # The multiple coverage logic is not needed for this endpoint, but we want to share this code between endpoints
        for (lat, lon, times), group in groupby(data, lambda x: x[0]):
            referencing = [
                ReferenceSystemConnectionObject(coordinates=["y", "x"],
                                                system=ReferenceSystem(type="GeographicCRS", id="http://www.opengis.net/def/crs/EPSG/0/4326")),
                ReferenceSystemConnectionObject(coordinates=["z"],
                                                system=ReferenceSystem(type="TemporalRS", calendar="Gregorian")),
            ]
            domain = Domain(domainType=DomainType.point_series,
                            axes=Axes(x=ValuesAxis[float](values=[lon]),
                                      y=ValuesAxis[float](values=[lat]),
                                      t=ValuesAxis[AwareDatetime](values=times)),
                            referencing=referencing)
            group1, group2 = itertools.tee(group, 2)  # Want to use generator twice
            parameters = {param_id: Parameter(observedProperty=ObservedProperty(label={"en": param_id}))
                          for ((_, _, _), param_id, values) in group1}
            ranges = {param_id: NdArray(values=values, axisNames=["t", "y", "x"], shape=[len(values), 1, 1])
                      for ((_, _, _), param_id, values) in group2}

            coverages.append(Coverage(domain=domain, parameters=parameters, ranges=ranges))

        if len(coverages) == 1:
            return coverages[0].model_dump(exclude_none=True)
        else:
            # HACK to take parameters from first one
            tmp = CoverageCollection(
                coverages=coverages, parameters=unpack_paramaters(coverages)).model_dump(exclude_none=True)
            return tmp

    def _parse_common_queriables(self, **kwargs):
        parsed_queryables = {}

        if kwargs["select_properties"]:
            parsed_queryables["standard_names"] = [i for i in kwargs["select_properties"]]

        if kwargs["bbox"]:
            points = []
            for i in kwargs["bbox"][::2]:
                for j in kwargs["bbox"][1::2]:
                    points.append(ds.Point(lat=i, lon=j))
            parsed_queryables["inside"] = ds.Polygon(points=points)

        if "inside" in parsed_queryables and not kwargs["bbox"]:
            points = list(map(lambda x: re.findall(r"\d+", x), parsed_queryables["inside"]))
            parsed_queryables["inside"] = [{"lat": i[0], "lon": j[0]}
                                           for i, j in zip(*[iter(points)]*2)]

        if kwargs["datetime_"]:
            if (datetime_ := kwargs["datetime_"]).endswith(","):
                end = Timestamp()
                start = dtime2tstamp(datetime_.split(",")[
                    0])
                end.FromSeconds(int(time.time()))
            else:
                start, end = list(map(dtime2tstamp, datetime_.split(",")))
            interval = ds.TimeInterval(
                start=start,
                end=end,
            )
            parsed_queryables["interval"] = interval

        return parsed_queryables

    def get(self, **kwargs):
        pass
        # obs_req = ds.GetObsReq(platforms=)

    def get_fields(self):
        rangetype = {
            "type": "DataRecord",
            "field": []
        }

        rangetype['field'].append({
            'id': "air_temperature",
            'type': 'Quantity',
                    'name': "air_temperature",
                    'encodingInfo': {
                        'dataType': 'https://github.com/EURODEO'
                    },
            'nodata': 'null',
            '_meta': {
                        'tags': ["ACDD", "CF"]
                    }
        })

        return rangetype

    def get_schema(self):
        tmp = [i for i in ds.GetObsRequest().DESCRIPTOR.fields_by_name.keys()]
        return tmp

# '{"interval": {"start": "2023-10-13T10:08:00Z", "end": "2023-10-13T10:10:10Z"}, "standard_names": "air_temperature"}


class gRPCprovider_records(gRPCprovider):
    """Provider for the E-SOH datastore grpc service"""

    def __init__(self, provider_def):
        """
        Docstring for grpcprovider poc
        """
        super().__init__(provider_def)

        self.fields = self.get_fields()

    def query(self, **kwargs):
        """
        Implement call to getObservations here?
                - What format does the API expect from this function
                - What is the input format expected in the edr class
                - geoJSON and coverageJSON?
                - bufr?

        """

        search_terms = {i[0]: i[1].split(",") for i in kwargs["properties"]}

        if "interval" in search_terms:
            ts_interval = search_terms.pop("interval")
            ts_interval = map(fix_timezone, ts_interval)
            search_terms["interval"] = {"start": "", "end": ""}
            search_terms["interval"]["start"], search_terms["interval"]["end"] = ts_interval
            # (lambda x: [dtime2tstamp(i) for i in x])(sorted(ts_interval))

        if kwargs["bbox"]:
            points = []
            for i in kwargs["bbox"][::2]:
                for j in kwargs["bbox"][1::2]:
                    points.append(ds.Point(lat=i, lon=j))
            search_terms["inside"] = ds.Polygon(points=points)

        if "inside" in search_terms and not kwargs["bbox"]:
            points = list(map(lambda x: re.findall(r"\d+", x), search_terms["inside"]))
            search_terms["inside"] = [{"lat": i[0], "lon": j[0]}
                                      for i, j in zip(*[iter(points)]*2)]

        if "inside" in search_terms:
            obs_req = ds.GetObsRequest(inside=search_terms.pop("inside"))
        else:
            obs_req = ds.GetObsRequest()
        json_format.ParseDict(search_terms, obs_req)

        try:
            request = self._stub.GetObservations(obs_req)
        except grpc.RpcError as e:
            LOGGER.critical(e)
            raise

        if not request.ListFields():
            return {}

        # TODO: remove duplicates from the list in locations

        features = {}
        for ts in request.observations:
            if ts.ts_mdata.platform in features:
                continue
            else:
                prop = json_format.MessageToDict(ts.ts_mdata)
                if "unit" in prop:
                    prop.pop("unit")
                if "instrument" in prop:
                    prop.pop("instrument")
                if "instruemnt_vocabluary" in prop:
                    prop.pop("instrument_vocabulary")
                if "standardName" in prop:
                    prop.pop("standardName")
                features[ts.ts_mdata.platform] = Feature(type="Feature", id=ts.ts_mdata.platform, properties=(prop),
                                                         geometry=Point(type="Point", coordinates=(ts.obs_mdata[0].geo_point.lon, ts.obs_mdata[0].geo_point.lat)))  # HACK: Assume loc the same
        features = list(features.values())

        return FeatureCollection(features=features, type="FeatureCollection").model_dump()

    def get(self, *args, **kwargs):
        obs_req = ds.GetObsRequest(platforms=[args[0]])

        try:
            request = self._stub.GetObservations(obs_req)
        except grpc.RpcError as e:
            LOGGER.critical(e)
            raise

        if not request.ListFields():
            return {}

        # TODO: remove duplicates from the list in locations
        for ts in request.observations:
            prop = json_format.MessageToDict(ts.ts_mdata)
            if "unit" in prop:
                prop.pop("unit")
            if "instrument" in prop:
                prop.pop("instrument")
            if "instruemnt_vocabluary" in prop:
                prop.pop("instrument_vocabulary")
            if "standardName" in prop:
                prop.pop("standardName")
            features = [
                Feature(type="Feature", id=ts.ts_mdata.platform, properties=(prop),
                        geometry=Point(type="Point", coordinates=(ts.obs_mdata[0].geo_point.lon, ts.obs_mdata[0].geo_point.lat)))  # HACK: Assume loc the same
                for ts in request.observations
            ]

        return features[0].model_dump()

    def get_fields(self):
        tmp = {i: {"type": "string"} for i in ds.GetObsRequest().DESCRIPTOR.fields_by_name.keys()}
        return tmp

    def get_schema(self):
        tmp = [i for i in ds.GetObsRequest().DESCRIPTOR.fields_by_name.keys()]
        return tmp

    def _load_and_prepare_item(self, item, identifier=None, accept_missing_identifier=False, raise_if_exists=True):
        return super()._load_and_prepare_item(item, identifier, accept_missing_identifier, raise_if_exists)
