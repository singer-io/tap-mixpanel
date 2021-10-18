#!/usr/bin/env python3

import sys
import json
import argparse
from datetime import datetime, timedelta, date
import singer
from singer import metadata, utils
from singer.utils import strptime_to_utc, strftime
from tap_mixpanel.client import MixpanelClient
from tap_mixpanel.discover import discover
from tap_mixpanel.sync import sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'project_timezone',
    'api_secret',
    'attribution_window',
    'start_date',
    'user_agent'
]


def do_discover(client, properties_flag):
    LOGGER.info('Starting discover')
    catalog = discover(client, properties_flag)
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info('Finished discover')


@singer.utils.handle_top_exception(LOGGER)
def main():
    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    start_date = parsed_args.config['start_date']
    start_dttm = strptime_to_utc(start_date)
    now_dttm = utils.now()
    if parsed_args.config.get('end_date'):
        now_dttm = strptime_to_utc(parsed_args.config.get('end_date'))
    delta_days = (now_dttm - start_dttm).days
    if delta_days >= 365:
        delta_days = 365
        start_date = strftime(now_dttm - timedelta(days=delta_days))
        LOGGER.warning("start_date greater than 1 year maxiumum for API.")
        LOGGER.warning("Setting start_date to 1 year ago, %s", start_date)

    #Check support for EU endpoints
    if str(parsed_args.config.get('eu_residency')).lower() == "true":
        api_domain = "eu.mixpanel.com"
    else:
        api_domain = "mixpanel.com"

    with MixpanelClient(parsed_args.config['api_secret'],
                        api_domain,
                        parsed_args.config['user_agent']) as client:

        state = {}
        if parsed_args.state:
            state = parsed_args.state

        config = parsed_args.config
        properties_flag = config.get('select_properties_by_default')

        if parsed_args.discover:
            client.__api_domain = api_domain
            do_discover(client, properties_flag)
        elif parsed_args.catalog:
            sync(client=client,
                 config=config,
                 catalog=parsed_args.catalog,
                 state=state,
                 start_date=start_date)


if __name__ == '__main__':
    main()
