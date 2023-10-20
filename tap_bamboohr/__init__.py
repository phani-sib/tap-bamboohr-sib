#!/usr/bin/env python3
import os
import json
import datetime
from datetime import timedelta
import singer
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
from tap_bamboohr.bamboohr_streams import BamboohrApi
import base64

REQUIRED_CONFIG_KEYS = ["api_key","subdomain", "date_from"]
LOGGER = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas

def get_bookmark(stream_id):
    bookmark = {
        "timeoffs": "latest_date",
    }
    return bookmark.get(stream_id)

def get_key_properties(stream_id):
    key_properties = {
        "employees_directory": ["id"],
        "timeoffs": ["id"],
    }
    return key_properties.get(stream_id, [])

def daterange(date1, date2):
    for n in range(int((date2 - date1).days)+1):
        yield date1 + timedelta(n)

def generate_dates_to_today(date_from_str:str):
    format = '%Y-%m-%d'
    date_from = datetime.datetime.strptime(date_from_str, format)-timedelta(days=15)
    date_to = datetime.datetime.today()

    for dt in daterange(date_from, date_to):
        yield dt.strftime(format)

def encode_api_key(api_key):

        sample_string = f"{api_key}:nothingtoseehere"
        sample_string_bytes = sample_string.encode("ascii")
        
        base64_bytes = base64.b64encode(sample_string_bytes)
        base64_string = base64_bytes.decode("ascii")
        
        return base64_string

def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        # if stream_id == "timeoffs":
            # TODO: populate any metadata and stream's key properties here..
            stream_metadata = []
            key_properties = get_key_properties(stream_id)
            streams.append(
                CatalogEntry(
                    tap_stream_id=stream_id,
                    stream=stream_id,
                    schema=schema,
                    key_properties=key_properties,
                    metadata=stream_metadata,
                    replication_key=None,
                    is_view=None,
                    database=None,
                    table=None,
                    row_count=None,
                    stream_alias=None,
                    replication_method=None,
                )
            )
    return Catalog(streams)


def sync(config, state, catalog):
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        if stream.tap_stream_id == "timeoffs":
            bookmark_column = get_bookmark(stream.tap_stream_id)
            bookmark_value = singer.get_bookmark(state, stream.tap_stream_id, bookmark_column)
            if bookmark_value is None:
                bookmark_value = config["date_from"]
        else:
            bookmark_column = False

        is_sorted = True  # TODO: indicate whether data is sorted ascending on bookmark value

        if stream.tap_stream_id not in BamboohrApi.bamboohr_streams:
            raise Exception(f"Unknown stream : {stream.tap_stream_id}")

        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=stream.schema.to_dict(),
            key_properties=stream.key_properties,
        )

        # TODO: delete and replace this inline function with your own data retrieval process:
        # tap_data = BamboohrApi.get_employees()
        encoded_api_key = encode_api_key(config["api_key"])
        bamboohr_client = BamboohrApi(encoded_api_key, config["subdomain"])

        max_bookmark = None
        if stream.tap_stream_id == "employees_directory":
            for record in bamboohr_client.get_sync_endpoints(stream.tap_stream_id, config["subdomain"]):
                # write one or more rows to the stream:
                singer.write_records(stream.tap_stream_id, record)
            # if bookmark_column:
            #     if is_sorted:
            #         # update bookmark to latest value
            #         singer.write_state({stream.tap_stream_id: bookmark_column})
            #     else:
            #         # if data unsorted, save max value until end of writes
            #         max_bookmark = max(max_bookmark, bookmark_column)

        elif stream.tap_stream_id == "timeoffs":
            for date_from in generate_dates_to_today(bookmark_value):
                for record in bamboohr_client.get_sync_endpoints(stream.tap_stream_id, config["subdomain"], parameters={'start': date_from,
                                                                'end': date_from}):
                    # write one or more rows to the stream:
                    singer.write_records(stream.tap_stream_id, record)
                if bookmark_column:
                    if is_sorted:
                        # update bookmark to latest value
                        state = singer.write_bookmark(
                            state, stream.tap_stream_id, bookmark_column, date_from
                        )
                        singer.write_state(state)
                    else:
                        # if data unsorted, save max value until end of writes
                        max_bookmark = max(max_bookmark, bookmark_column)

        if bookmark_column and not is_sorted:
            singer.write_state({stream.tap_stream_id: max_bookmark})
    return


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
