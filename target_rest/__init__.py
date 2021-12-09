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

REQUIRED_CONFIG_KEYS = ["api_url"]

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
    """
    Send json data to REST endpoint at url
    :param data: JSON encoded data to be sent
    :param url: URL of endpoint where to send the data
    """
    # TODO: Handle authentification
    # TODO: Handle bad URL and issues with REST server
    r = requests.post(url, json=data)

    # If the response is not OK rise exception
    if not r.ok:
        raise Exception(f'REST API on {url} returned status code {r.status_code}')

    # TODO: r.ok is True for status_code < 400. 
    # Think about status_codes < 400 that can couse problems like `204 - No content` and handle them
    # https://en.wikipedia.org/wiki/List_of_HTTP_status_codes

def batch_data(data_container, new_data):
    """
    Adds new data into the batch for batch processing
    :param data_container: Dictionary with batched data
    :param new_data: New data to be added to the batch
    :return: Number of items currently in batch after new data was inserted
    """
    data_keys = list(new_data.keys())
    # TODO: Make this safer (i.e. when the keys are missing and so on)
    if len(data_container) == 0:
        # If data container is empty, create structure for it
        for key in data_keys:
            data_container[key] = [new_data[key]]
    else:
        # Else add new data to lists
        for key in data_keys:
            data_container[key].append(new_data[key])
    return len(data_container[key])

def persist_lines(config, lines):
    """
    Sends data to server line by line or in batches if `bat_size` in configuration is set.
    :param config: Target configuration
    :param lines: Lines with JSON data from tap
    :return: last state
    """
    state = None
    schemas = {}
    key_properties = {}
    headers = {}
    validators = {}

    batch_size = config.get('batch_size', None)
    data = {}
    
    now = datetime.now().strftime('%Y%m%dT%H%M%S')

    # Loop over lines from stdin
    for line in lines:
        # Try to parse json data
        try:
            message = singer.parse_message(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise
        
        # Handle single record in message
        if  isinstance(message, singer.RecordMessage):
            stream = message.stream

            if stream not in schemas:
                raise Exception("A record for stream {} was encountered before a corresponding schema".format(json_object['stream']))

            # Get schema for this record's stream
            schema = schemas[stream]

            # Get the record from message
            record = message.record

            # Validate record
            validators[stream].validate(record)

            # Send data to REST server
            if batch_size is None or batch_size == 1:
                # Send one line of data if no batching is configured
                send_data(record, config['api_url'])
            else:
                #Otherwise batch data and send once there is enough in container
                data_length = batch_data(data, record)
                if data_length >= batch_size:
                    send_data(data, config['api_url'])
                    data = {}

            state = None

        elif isinstance(message, singer.StateMessage):
            state = message.value
            logger.debug(f'Setting state to {state}')

        elif isinstance(message, singer.SchemaMessage):
            stream = message.stream
            schemas[stream] = message.schema
            validators[stream] = Draft4Validator(message.schema)
            key_properties[stream] = message.key_properties
        elif isinstance(message, singer.ActivateVersionMessage):
            # This is a signal to the Target that it should delete all previously
            # seen data and replace it with all the RECORDs it has seen where the
            # record's version matches this version number.
            
            # TODO: To implement this could be tricky. We would need specific enpoint 
            # that would delete all
            
            logger.warning('Ignoring ActivateVersionMessage')
        else:
            raise Exception(f'Unknown message type {message.type} in message {message}')
    
    # Send last batch that is smaller then batch_size (for cases when data_size % batch_size != 0)
    # TODO: This seem a little slopy, maybe better would be cover this directly in loop over the lines
    if len(data) >= 0 and batch_size is not None and batch_size > 1:
        send_data(data, config['api_url'])
    
    return state


def send_usage_stats():
    """
    Send anonymous usage data to singer.io
    """
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
    """
    Main function that parse configuration and start sending data to REST server
    """
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

    singer.utils.check_config(config, REQUIRED_CONFIG_KEYS)

    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = persist_lines(config, input)
        
    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
