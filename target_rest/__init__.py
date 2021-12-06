#!/usr/bin/env python3

import argparse
import io
import os
import sys
import json
import threading
import http.client
import urllib
import requests
from datetime import datetime
import collections

import pkg_resources
from jsonschema.validators import Draft4Validator
import singer

logger = singer.get_logger()

def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()

def flatten(d, parent_key='', sep='__'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, str(v) if type(v) is list else v))
    return dict(items)

def send_data(data, url):
    # TODO: Handle authentification
    # TODO: Handle bad URL and issues with REST server
    r = requests.post(url, json=data)

    # TODO: Handle response codes and react based on what gets back
    if r.status_code == 200:
        return True
    return False

def persist_lines(config, lines):
    state = None
    schemas = {}
    key_properties = {}
    headers = {}
    validators = {}
    
    now = datetime.now().strftime('%Y%m%dT%H%M%S')

    # Loop over lines from stdin
    for line in lines:
        try:
            json_object = json.loads(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        if 'type' not in json_object:
            raise Exception("Line is missing required key 'type': {}".format(line))
        message_type = json_object['type']

        if message_type == 'RECORD':
            if 'stream' not in json_object:
                raise Exception("Line is missing required key 'stream': {}".format(line))
            if json_object['stream'] not in schemas:
                raise Exception("A record for stream {} was encountered before a corresponding schema".format(json_object['stream']))

            # Get schema for this record's stream
            schema = schemas[json_object['stream']]

            # Validate record
            validators[json_object['stream']].validate(json_object['record'])

            # TODO: Implement batching
            # Send data to REST server
            send_data(json_object['record'], config['api_url'])

            state = None
        elif message_type == 'STATE':
            logger.debug('Setting state to {}'.format(json_object['value']))
            state = json_object['value']
        elif message_type == 'SCHEMA':
            if 'stream' not in json_object:
                raise Exception("Line is missing required key 'stream': {}".format(line))
            stream = json_object['stream']
            schemas[stream] = json_object['schema']
            validators[stream] = Draft4Validator(json_object['schema'])
            if 'key_properties' not in json_object:
                raise Exception("key_properties field is required")
            key_properties[stream] = json_object['key_properties']
        else:
            raise Exception("Unknown message type {} in message {}"
                            .format(json_object['type'], json_object))
    
    return state


def send_usage_stats():
    try:
        version = pkg_resources.get_distribution('target-rest-api').version
        conn = http.client.HTTPConnection('collector.singer.io', timeout=10)
        conn.connect()
        params = {
            'e': 'se',
            'aid': 'singer',
            'se_ca': 'target-rest-api',
            'se_ac': 'open',
            'se_la': version,
        }
        conn.request('GET', '/i?' + urllib.parse.urlencode(params))
        response = conn.getresponse()
        conn.close()
    except:
        logger.debug('Collection request failed')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file')
    args = parser.parse_args()

    if args.config:
        with open(args.config) as input:
            config = json.load(input)
    else:
        config = {}

    if not config.get('disable_collection', False):
        logger.info('Sending version information to singer.io. ' +
                    'To disable sending anonymous usage data, set ' +
                    'the config parameter "disable_collection" to true')
        threading.Thread(target=send_usage_stats).start()

    # TODO: Check if api_url is in config

    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = persist_lines(config, input)
        
    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
